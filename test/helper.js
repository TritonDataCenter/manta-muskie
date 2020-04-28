/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var fs = require('fs');
var http = require('http');
var https = require('https');
var path = require('path');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var forkExecWait = require('forkexec').forkExecWait;
var manta = require('manta');
var qlocker = require('qlocker');
var restifyClients = require('restify-clients');
var sshpk = require('sshpk');
var UFDS = require('ufds');
var uuidv4 = require('uuid/v4');
var vasync = require('vasync');
var VError = require('verror');

var auth = require('../lib/auth');


///--- Globals

// XXX can we drop these?
http.globalAgent.maxSockets = 50;
https.globalAgent.maxSockets = 50;


/*
 * We need a regular (non-operator) account for some tests.  The regular
 * Manta client environment variables (MANTA_USER, MANTA_KEY_ID) are used
 * for this account.  Allow the private key to be stored at a location
 * other than the default "$HOME/.ssh/id_rsa" file:
 */
var TEST_REGULAR_KEY = process.env.MUSKIETEST_REGULAR_KEYFILE ||
        (process.env.HOME + '/.ssh/id_rsa');

/*
 * We need an operator account for some tests, so we use poseidon, unless an
 * alternate one is provided.
 */
var TEST_OPERATOR = process.env.MUSKIETEST_OPERATOR_USER || 'poseidon';
var TEST_OPERATOR_KEY;

// If MUSKIETEST_OPERATOR_KEYFILE is set, make sure file exists.
if (process.env.MUSKIETEST_OPERATOR_KEYFILE) {
    if (fs.existsSync(process.env.MUSKIETEST_OPERATOR_KEYFILE)) {
        TEST_OPERATOR_KEY = process.env.MUSKIETEST_OPERATOR_KEYFILE;
    } else {
        console.error('MUSKIETEST_OPERATOR_KEYFILE %s does not exist!',
                      process.env.MUSKIETEST_OPERATOR_KEYFILE);
        process.exit(1);
    }
} else {
    TEST_OPERATOR_KEY = (process.env.HOME + '/.ssh/id_rsa_poseidon');
}


///--- Helpers

function getRegularPubkey() {
    return (fs.readFileSync(TEST_REGULAR_KEY + '.pub', 'utf8'));
}

function getRegularPrivkey() {
    return (fs.readFileSync(TEST_REGULAR_KEY, 'utf8'));
}

function getOperatorPubkey() {
    return (fs.readFileSync(TEST_OPERATOR_KEY + '.pub', 'utf8'));
}

function getOperatorPrivkey() {
    return (fs.readFileSync(TEST_OPERATOR_KEY, 'utf8'));
}

function getKeyFingerprint(key) {
    return (sshpk.parseKey(key, 'auto').fingerprint('md5').toString());
}

function createLogger(name) {
    assert.optionalString(name, 'name');

    var log = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'warn'),
        name: name || process.argv[1],
        stream: process.stdout,
        src: true,
        serializers: restifyClients.bunyan.serializers
    });
    return (log);
}


function mantaClientFromAccountInfo(accountInfo) {
    assert.object(accountInfo, 'accountInfo');
    assert.string(accountInfo.login, 'accountInfo.login');
    assert.string(accountInfo.fp, 'accountInfo.fp');
    assert.string(accountInfo.privKey, 'accountInfo.privKey');
    assert.string(process.env.MANTA_URL, 'process.env.MANTA_URL');

    var log = createLogger();
    var client = manta.createClient({
        agent: false,
        connectTimeout: 2000,
        log: log,
        retry: false,
        sign: manta.privateKeySigner({
            key: accountInfo.privKey,
            keyId: accountInfo.fp,
            log: log,
            user: accountInfo.login
        }),
        rejectUnauthorized: false,
        url: process.env.MANTA_URL,
        user: accountInfo.login
    });

    return (client);
}


function createUserClient(login) {
    assert.string(process.env.MANTA_URL, 'process.env.MANTA_URL');
    assert.string(process.env.MANTA_USER, 'process.env.MANTA_USER');
    assert.string(process.env.MANTA_KEY_ID, 'process.env.MANTA_KEY_ID');

    var key = getRegularPrivkey();
    var log = createLogger();
    var client = manta.createClient({
        agent: false,
        connectTimeout: 2000,
        log: log,
        retry: false,
        sign: manta.privateKeySigner({
            key: key,
            keyId: process.env.MANTA_KEY_ID,
            log: log,
            user: process.env.MANTA_USER,
            subuser: login
        }),
        rejectUnauthorized: false,
        url: process.env.MANTA_URL,
        user: process.env.MANTA_USER,
        subuser: login
    });

    return (client);
}


function createJsonClient() {
    assert.string(process.env.MANTA_URL, 'process.env.MANTA_URL');

    var log = createLogger();
    var client = restifyClients.createClient({
        agent: false,
        connectTimeout: 250,
        log: log,
        rejectUnauthorized: false,
        retry: false,
        type: 'json',
        url: process.env.MANTA_URL
    });

    return (client);
}

function createStringClient() {
    assert.string(process.env.MANTA_URL, 'process.env.MANTA_URL');

    var client = restifyClients.createClient({
        agent: false,
        connectTimeout: 250,
        log: createLogger(),
        rejectUnauthorized: false,
        retry: false,
        type: 'string',
        url: process.env.MANTA_URL
    });

    return (client);
}


// XXX
//function createSDCClient() {
//    assert.string(process.env.SDC_URL, 'process.env.SDC_URL');
//    assert.string(process.env.SDC_ACCOUNT, 'process.env.SDC_ACCOUNT');
//    assert.string(process.env.SDC_KEY_ID, 'process.env.SDC_KEY_ID');
//
//    var key = getRegularPrivkey();
//    var log = createLogger();
//    var client = smartdc.createClient({
//        log: log,
//        sign: smartdc.privateKeySigner({
//            key: key,
//            keyId: process.env.SDC_KEY_ID,
//            user: process.env.SDC_ACCOUNT
//        }),
//        rejectUnauthorized: false,
//        user: process.env.SDC_ACCOUNT,
//        url: process.env.SDC_URL
//    });
//
//    return (client);
//}
//
//function createOperatorSDCClient() {
//    assert.string(process.env.SDC_URL, 'process.env.SDC_URL');
//
//    var key = getOperatorPrivkey();
//    var keyId = getKeyFingerprint(key);
//
//    var log = createLogger();
//    var client = smartdc.createClient({
//        log: log,
//        sign: smartdc.privateKeySigner({
//            key: key,
//            keyId: keyId,
//            log: log,
//            user: TEST_OPERATOR
//        }),
//        rejectUnauthorized: false,
//        version: '9.0.0',
//        url: process.env.SDC_URL,
//        user: TEST_OPERATOR
//    });
//
//    return (client);
//}

function createOperatorClient() {
    assert.string(process.env.MANTA_URL, 'process.env.MANTA_URL');

    var key = getOperatorPrivkey();
    var keyId = getKeyFingerprint(key);

    var log = createLogger();
    var client = manta.createClient({
        agent: false,
        connectTimeout: 2000,
        log: log,
        retry: false,
        sign: manta.privateKeySigner({
            key: key,
            keyId: keyId,
            log: log,
            user: TEST_OPERATOR
        }),
        rejectUnauthorized: false,
        url: process.env.MANTA_URL,
        user: TEST_OPERATOR
    });

    return (client);
}

function checkResponse(t, res, code) {
    t.ok(res, 'null response');
    if (!res)
        return;
    t.equal(res.statusCode, code, 'HTTP status code mismatch');
    t.ok(res.headers, 'has headers');
    t.ok(res.headers.date, 'headers have date');
    t.equal(res.headers.server, 'Manta/2', 'server header is Manta/2');
    t.ok(res.headers['x-request-id'], 'headers have x-req-id');
    t.ok(res.headers['x-server-name'], 'headers have x-server-name');

    if (code === 200 || code === 201 || code === 202) {
        t.ok(res.headers['content-type'], 'headers have content-type');
        var ct = res.headers['content-type'];
        /* JSSTYLED */
        if (!/application\/x-json-stream.*/.test(ct)) {
            t.ok(res.headers['content-length'] !== undefined);
            if (res.headers['content-length'] > 0)
                t.ok(res.headers['content-md5']);
        }
    }
}


// XXX can we use a manta client for this? Should be able to.
//function signRequest(opts, cb) {
//    var key = opts.key || getRegularPrivkey();
//
//    var sign = manta.privateKeySigner({
//        key: key,
//        keyId: opts.keyId || process.env.MANTA_KEY_ID,
//        user: opts.user || process.env.MANTA_USER
//    });
//
//    var rs = smartdc_auth.requestSigner({
//        sign: sign,
//        mantaSubUser: true
//    });
//
//    var date = rs.writeDateHeader();
//
//    rs.sign(function gotSignature(err, authz) {
//        if (err) {
//            cb(err);
//            return;
//        }
//        cb(null, authz, date);
//    });
//}
//
//function signUrl(opts, expires, cb) {
//    if (typeof (opts) === 'string') {
//        opts = { path: opts };
//    }
//    if (typeof (expires) === 'function') {
//        cb = expires;
//        expires = Date.now() + (1000 * 300);
//    }
//    var key = getRegularPrivkey();
//    var keyId = process.env.MANTA_KEY_ID;
//    var url = process.env.MANTA_URL || 'http://localhost:8080';
//    var user = process.env.MANTA_USER;
//    var subuser = process.env.MANTA_SUBUSER;
//
//    if (opts.client) {
//        user = opts.client.user;
//        subuser = opts.client.subuser;
//    }
//
//    manta.signUrl({
//        algorithm: 'rsa-sha256',
//        expires: expires,
//        host: require('url').parse(url).host,
//        keyId: keyId,
//        method: opts.method || 'GET',
//        path: opts.path,
//        role: opts.role,
//        query: opts.query,
//        'role-tag': opts['role-tag'],
//        sign: manta.privateKeySigner({
//            algorithm: 'rsa-sha256',
//            key: key,
//            keyId: keyId,
//            log: createLogger(),
//            user: user,
//            subuser: subuser
//        }),
//        user: user,
//        subuser: subuser
//    }, cb);
//}




function _ensureAccount(opts, cb) {
    assert.object(opts.t, 'opts.t');
    assert.object(opts.ufdsClient, 'opts.ufdsClient');
    assert.bool(opts.isOperator, 'opts.isOperator');
    assert.string(opts.login, 'opts.login');
    assert.string(opts.cacheDir, 'opts.cacheDir');

    var t = opts.t;
    var info;

    vasync.pipeline({arg: {}, funcs: [
        function ensureTheAccount(ctx, next) {
            opts.ufdsClient.getUserEx({
                searchType: 'login',
                value: opts.login
            }, function (getErr, account) {
                if (getErr && getErr.name !== 'ResourceNotFoundError') {
                    next(new VError(getErr,
                        'unexpected error loading account "%s"', opts.login));
                    return;
                } else if (account) {
                    ctx.account = account;
                    t.comment(`already have account "${opts.login}"`);
                    next();
                } else {
                    opts.ufdsClient.addUser({
                        login: opts.login,
                        email: opts.login + '@localhost',
                        userpassword: uuidv4(),
                        approved_for_provisioning: true
                    }, function (addErr, newAccount) {
                        if (addErr) {
                            next(new VError(addErr,
                                'could not create account "%s"', opts.login));
                        } else {
                            t.comment(`created account "${opts.login}"`);
                            ctx.account = newAccount;
                            next();
                        }
                    });
                }
            });
        },

        function addToOperatorsIfRequested(ctx, next) {
            if (!opts.isOperator) {
                next();
                return;
            }

            ctx.account.addToGroup('operators', next);
        },

        function ensureTheKey(ctx, next) {
            ctx.privKeyPath = path.join(opts.cacheDir, opts.login + '.id_rsa');
            if (!fs.existsSync(ctx.privKeyPath)) {
                var argv = [
                    'ssh-keygen',
                    '-t', 'rsa',
                    '-C', opts.login,
                    '-b', '2048',
                    '-N', '',
                    '-f', ctx.privKeyPath
                ];
                forkExecWait({
                    argv: argv,
                    includeStderr: true
                }, function (err, info) {
                    if (err) {
                        next(new VError(err,
                            'failed to generate key for login "%s"',
                            opts.login));
                    } else {
                        t.comment(`created new key "${ctx.privKeyPath}"`);
                        ctx.privKey = fs.readFileSync(ctx.privKeyPath, 'utf8');
                        ctx.pubKey = fs.readFileSync(ctx.privKeyPath + '.pub',
                            'utf8');
                        next();
                    }
                });
            } else {
                ctx.privKey = fs.readFileSync(ctx.privKeyPath, 'utf8');
                ctx.pubKey = fs.readFileSync(ctx.privKeyPath + '.pub', 'utf8');
                next();
            }
        },

        function ensureKeyOnAccount(ctx, next) {
            ctx.fp = getKeyFingerprint(ctx.pubKey);
            opts.ufdsClient.getKey(ctx.account, ctx.fp, function (getErr, key) {
                if (getErr && getErr.name !== 'ResourceNotFoundError') {
                    next(new VError(getErr,
                        'unexpected error checking for key on account "%s"',
                        opts.login));
                } else if (getErr) {
                    opts.ufdsClient.addKey(ctx.account, ctx.pubKey, function (addErr) {
                        if (addErr) {
                            next(new VError(addErr,
                                'could not add key to account "%s"',
                                opts.login));
                        } else {
                            t.comment(`added key "${ctx.fp}" to account`);
                            next();
                        }
                    });
                } else {
                    next();
                }
            });
        },

        function buildInfo(ctx, next) {
            info = {
                login: opts.login,
                uuid: ctx.account.uuid,
                pubKey: ctx.pubKey,
                privKey: ctx.privKey,
                fp: ctx.fp,
                isOperator: opts.isOperator
            };
            next();
        }

    ]}, function finish(err) {
        if (err) {
            cb(err);
        } else {
            cb(null, info);
        }
    });
}


// Wait for the given account to be "ready". Here "ready" means that we get
// a successful response from `HEAD /:login/stor`.
function _waitForAccountToBeReady(t, opts, cb) {
    assert.object(t, 't');
    assert.object(opts.accountInfo, 'opts.accountInfo');
    assert.finite(opts.timeout, 'opts.timeout'); // number of seconds

    var client = mantaClientFromAccountInfo(opts.accountInfo);
    var lastErr = true; // `true` to force first `pingAttempt`
    var start = Date.now();

    // Currently we wait forever here, relying on test timeouts.
    vasync.whilst(
        function notYetWorking() {
            return !!lastErr;
        },
        function pingAttempt(attemptCb) {
            if ((Date.now() - start) / 1000 > opts.timeout) {
                cb(new VError(
                    'reached %ds timeout waiting for account "%s" to be ready',
                    opts.timeout, opts.accountInfo.login));
                return;
            }

            var p = '/' + opts.accountInfo.login + '/stor';
            client.info(p, function (err, info, res) {
                if (res === undefined) {
                    // MantaClient.info callback is crazy this way.
                    res = info;
                }
                t.comment(`[${new Date().toISOString()}] ` +
                    `HEAD ${p} -> ${res ? res.statusCode : '<no response>'}`);
                lastErr = err;
                if (err) {
                    // Short delay before the next attempt.
                    setTimeout(attemptCb, 1000);
                } else {
                    attemptCb();
                }
            });
        },
        function done(err) {
            client.close();
            cb(err);
        }
    )
}


// Many muskie integration tests require a test account (and some a separate
// test *operator* account). This function will ensure those are created and
// ready and return the relevant info. An interprocess lock is used to
// coordinate between concurrent test runs.
//
// The created accounts are left between runs (data is in /var/db/muskietest/),
// which is a side-effect of running this test suite. As a result, this
// shouldn't be used in production. As a sanity guard against someone running
// the muskie test suite in prod, this function will abort if
// `metadata.SIZE == "production"`.
//
// The created accounts are:
// - muskietest_account_$firstPartOfInstanceUuid
// - muskietest_operator_$firstPartOfInstanceUuid
//
// XXX rbac stuff
function ensureTestAccounts(t, cb) {
    const DB_DIR = '/var/db/muskietest';
    var context = {
        dbDir: DB_DIR,
        lockFile: path.join(DB_DIR, 'accounts.lock'),
        // XXX
        //infoFile: path.join(DB_DIR, 'accounts.json'),
        accounts: {}
    };

    vasync.pipeline({
        arg: context,
        funcs: [
            // Abort if looks like it is production, because this function
            // has side-effects (creating accounts).
            function productionRunGuard(ctx, next) {
                try {
                    var metadata = JSON.parse(
                        fs.readFileSync('/var/tmp/metadata.json'));
                } catch (err) {
                    next(new VError(err,
                        'cannot read "/var/tmp/metadata.json"'));
                    return;
                }

                ctx.instUuid = metadata.INSTANCE_UUID;
                assert.uuid(ctx.instUuid, 'metadata.INSTANCE_UUID');

                if (!metadata.SIZE) {
                    next(new VError('metadata from "/var/tmp/metadata.json" ' +
                        'does not include a "SIZE" var'));
                } else if (metadata.SIZE === 'production') {
                    next(new VError('metadata.SIZE==="production": ' +
                        'refusing to create test accounts'));
                } else {
                    next();
                }
            },

            function mkdirpDbDir(ctx, next) {
                fs.mkdir(ctx.dbDir, function onMkdir(err) {
                    if (err && err.code !== 'EEXIST') {
                        next(err);
                    } else {
                        next();
                    }
                });
            },
            function getLock(ctx, next) {
                qlocker.lock(ctx.lockFile, function (err, unlockFn) {
                    t.comment(`[${new Date().toISOString()}] ` +
                        `acquired test accounts lock`);
                    ctx.unlockFn = unlockFn;
                    next(err);
                });
            },

            // XXX
            //function loadAccountInfo(ctx, next) {
            //    fs.readFile(ctx.infoFile, function (err, data) {
            //        if (err) {
            //            if (err.code === 'ENOENT') {
            //                ctx.accounts = null;
            //                next();
            //            } else {
            //                next(err);
            //            }
            //        } else {
            //            try {
            //                ctx.accounts = JSON.parse(data);
            //            } catch (parseErr) {
            //                next(new VError(parseErr,
            //                    'could not parse muskie test account info ' +
            //                    'file "%s" (delete it and re-run tests)',
            //                    ctx.infoFile));
            //                return;
            //            }
            //            t.comment('loaded test account info from cache:' +
            //                ctx.infoFile);
            //            next();
            //        }
            //    });
            //},
            //function sanityCheckAccountInfo(ctx, next) {
            //    if (!ctx.accounts) {
            //        next();
            //        return;
            //    }
            //
            //    // We have account info from an earlier run. Do a sanity
            //    // check that it works, then return.
            //    XXX
            //    var mantaClient = getMantaClient(ctx.accounts);
            //    var login = ctx.accounts.login;
            //    var p = '/' + login + '/stor';
            //    mantaClient.info(p, function (err, info) {
            //        console.log('XXX err', err);
            //        console.log('XXX info', info);
            //        if (err) {
            //            delete ctx.accounts;
            //            // XXX If we error out here, then a broken test account
            //            //     will require the developer to manually recover.
            //            //     That could be a PITA.
            //            next(new VError(err,
            //                'could not verify that test user "%s" is usable',
            //                login));
            //        } else {
            //            next(true);  // early abort
            //        }
            //    });
            //},
            //
            //// If we get this far, then we need to create the test accounts.

            // There is a guard above against running in production, so we can
            // (hopefully) assume this DC's UFDS is the master. If not, then
            // we'll be creating accounts in a non-master UFDS which breaks UFDS
            // replication.
            function getUfdsClient(ctx, next) {
                try {
                    var muskieConfig = JSON.parse(
                        fs.readFileSync('/opt/smartdc/muskie/etc/config.json'));
                } catch (err) {
                    next(new VError(err, 'cannot load muskie config'));
                    return;
                }

                muskieConfig.ufds.log = createLogger('ufdsClient');

                ctx.ufdsClient = new UFDS(muskieConfig.ufds);
                ctx.ufdsClient.once('error', next);
                ctx.ufdsClient.once('connect', function () {
                    ctx.ufdsClient.removeAllListeners('error');
                    ctx.ufdsClient.on('error', function (err) {
                        throw new VError(err, 'ufdsClient error');
                    });
                    next();
                });
            },

            // Create (or load) the 'muskietest_account_...' account.
            function ensureRegularAccount(ctx, next) {
                // We put (part of) the instance UUID in the test account
                // to balance between (a) not creating zillions of test accounts
                // on re-runs and (b) not having test runs from separate muskie
                // instances collide with each other.
                var login = 'muskietest_account_' + ctx.instUuid.split('-')[0];

                // XXX RBAC info
                _ensureAccount({
                    t: t,
                    ufdsClient: ctx.ufdsClient,
                    login: login,
                    isOperator: false,
                    cacheDir: ctx.dbDir
                }, function (err, info) {
                    if (err) {
                        next(err);
                    } else {
                        t.ok(info, 'ensured regular account: login=' +
                            info.login + ', ...');
                        ctx.accounts.regular = info;
                        next();
                    }
                });
            },


            // Create (or load) the 'muskietest_operator_...' account.
            function ensureOperatorAccount(ctx, next) {
                var login = 'muskietest_operator_' + ctx.instUuid.split('-')[0];

                // XXX RBAC info
                _ensureAccount({
                    t: t,
                    ufdsClient: ctx.ufdsClient,
                    login: login,
                    isOperator: true,
                    cacheDir: ctx.dbDir
                }, function (err, info) {
                    if (err) {
                        next(err);
                    } else {
                        t.ok(info, 'ensured operator account: login=' +
                            info.login + ', ...');
                        ctx.accounts.operator = info;
                        next();
                    }
                });
            },

            function waitForRegularAccountToBeReady(ctx, next) {
                _waitForAccountToBeReady(t, {
                    accountInfo: ctx.accounts.regular,
                    timeout: 60
                }, next);
            },

            function waitForOperatorAccountToBeReady(ctx, next) {
                _waitForAccountToBeReady(t, {
                    accountInfo: ctx.accounts.operator,
                    timeout: 60
                }, next);
            },

            // XXX save info out to info file
            // XXX test with concurrent tests

            // XXX docs in README on side-effects of testing (creating the user)
            // XXX tooling to cleanly delete the test user
        ]
    }, function finish(err) {
        if (err === true) {
            // Early abort signal.
            err = null;
        }

        // Cleanup and callback.
        vasync.pipeline({funcs: [
            function cleanupLock(_, next) {
                if (context.unlockFn) {
                    t.comment(`[${new Date().toISOString()}] ` +
                        `releasing test accounts lock`);
                    context.unlockFn(next);
                } else {
                    next();
                }
            },
            function cleanupUfdsClient(_, next) {
                if (context.ufdsClient) {
                    context.ufdsClient.close();
                }
                next();
            },
        ]}, function (cleanupErr) {
            t.ifError(cleanupErr, 'no error cleaning up ensureTestAccounts');
            cb(err, context.accounts);
        });
    });
}

///--- Exports

module.exports = {
    TEST_OPERATOR: TEST_OPERATOR,

    mantaClientFromAccountInfo: mantaClientFromAccountInfo,


    createJsonClient: createJsonClient,
    createStringClient: createStringClient,
    createUserClient: createUserClient,
    createLogger: createLogger,
    createOperatorClient: createOperatorClient,
    ensureTestAccounts: ensureTestAccounts,
    getRegularPubkey: getRegularPubkey,
    getRegularPrivkey: getRegularPrivkey,
    getOperatorPubkey: getOperatorPubkey,
    getOperatorPrivkey: getOperatorPrivkey,
    getKeyFingerprint: getKeyFingerprint
};
