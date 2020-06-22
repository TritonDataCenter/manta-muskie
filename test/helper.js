/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var format = require('util');
var fs = require('fs');
var http = require('http');
var https = require('https');
var path = require('path');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var forkExecWait = require('forkexec').forkExecWait;
var glob = require('glob');
var jsprim = require('jsprim');
var manta = require('manta');
var qlocker = require('qlocker');
var restifyClients = require('restify-clients');
var smartdcAuth = require('smartdc-auth');
var sshpk = require('sshpk');
var UFDS = require('ufds');
var uuidv4 = require('uuid/v4');
var vasync = require('vasync');
var VError = require('verror');

// The relatively large smartdc dep is only required for `_ensureRbacSettings()`
// used only for "test/integration/ac.test.js". A more modern option would be
// to use node-triton. However its RBAC support is fledgling and node-triton
// would be an even *bigger* dep.
var smartdc = require('smartdc');

var auth = require('../lib/auth');


///--- Globals

const DB_DIR = '/var/db/muskietest';
const ACCOUNTS_LOCK_FILE = path.join(DB_DIR, 'accounts.lock');


///--- Helpers

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


function mantaClientFromSubuserInfo(accountInfo, subuserLogin) {
    assert.object(accountInfo, 'accountInfo');
    assert.string(accountInfo.login, 'accountInfo.login');
    assert.string(accountInfo.fp, 'accountInfo.fp');
    assert.string(accountInfo.privKey, 'accountInfo.privKey');
    assert.string(subuserLogin, 'subuserLogin');
    assert.string(process.env.MANTA_KEY_ID, 'process.env.MANTA_KEY_ID');

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
            user: accountInfo.login,
            subuser: subuserLogin
        }),
        rejectUnauthorized: false,
        url: process.env.MANTA_URL,
        user: accountInfo.login,
        subuser: subuserLogin
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


function assertMantaRes(t, res, code) {
    t.ok(res, 'have a response object');
    if (!res)
        return;
    t.equal(res.statusCode, code, 'HTTP response status code is ' + code);
    t.ok(res.headers, 'has headers');
    t.ok(res.headers.date, 'have "date" header');
    t.equal(res.headers.server, 'Manta/2', 'server header is Manta/2');
    t.ok(res.headers['x-request-id'], 'headers have x-request-id');
    t.ok(res.headers['x-server-name'], 'headers have x-server-name');

    if (code === 200 || code === 201 || code === 202) {
        t.ok(res.headers['content-type'], 'headers have content-type');
        var ct = res.headers['content-type'];
        /* JSSTYLED */
        if (!/application\/x-json-stream.*/.test(ct)) {
            t.ok(res.headers['content-length'] !== undefined);
            if (res.headers['content-length'] > 0) {
                t.ok(res.headers['content-md5']);
            }
        }
    }
}


// Generate Manta http-signature headers for a request, using the auth info
// from the given account info. This `accountInfo` is an object of the form
// returned by `helper.ensureTestAccounts()`.
//
// Calls back with `cb(err)` or `cb(null, authz, date)` where `authz` is
// the "Authorization" header and `date` is the "Date" header to use for the
// signed request.
function signReq(accountInfo, cb) {
    assert.object(accountInfo, 'accountInfo');
    assert.string(accountInfo.privKey, 'accountInfo.privKey');
    assert.string(accountInfo.fp, 'accountInfo.fp');
    assert.string(accountInfo.login, 'accountInfo.login');

    var keySigner = manta.privateKeySigner({
        key: accountInfo.privKey,
        keyId: accountInfo.fp,
        user: accountInfo.login
    });
    var reqSigner = smartdcAuth.requestSigner({
        sign: keySigner,
        mantaSubUser: true
    });
    var date = reqSigner.writeDateHeader();

    reqSigner.sign(function onSigned(err, authz) {
        if (err) {
            cb(err);
        } else {
            cb(null, authz, date);
        }
    });
}

// Ensure the RBAC settings (subusers, policies, roles) on the given account
// are set as given.
function _ensureRbacSettings(opts, cb) {
    assert.object(opts.t, 'opts.t');
    assert.object(opts.account, 'opts.account');
    assert.arrayOfObject(opts.subusers, 'opts.subusers');
    assert.arrayOfObject(opts.policies, 'opts.policies');
    assert.arrayOfObject(opts.roles, 'opts.roles');
    assert.func(cb, 'cb');

    var accUuid = opts.account.uuid;
    var context = {
        madeAdditions: false,
        madeRoleAdditions: false
    };
    var t = opts.t;

    vasync.pipeline({arg: context, funcs: [
        function createSmartdcClient(ctx, next) {
            try {
                var muskieConfig = JSON.parse(
                    fs.readFileSync('/opt/smartdc/muskie/etc/config.json'));
            } catch (err) {
                next(new VError(err, 'cannot load muskie config'));
                return;
            }

            // ldaps://ufds.$datacenter_name.$dns_domain
            // -> https://cloudapi.$datacenter_name.$dns_domain
            var cloudapiUrl = muskieConfig.ufds.url
                .replace(/^ldaps/, 'https')
                .replace(/ufds\./, 'cloudapi.');

            ctx.smartdcClient = smartdc.createClient({
                log: createLogger('smartdc'),
                sign: smartdc.privateKeySigner({
                    key: opts.account.privKey,
                    keyId: opts.account.fp,
                    user: opts.account.login
                }),
                rejectUnauthorized: false,
                user: opts.account.login,
                // This elects to use the newer arg format for CreateRole.
                // See https://apidocs.joyent.com/cloudapi/#900
                version: '~9',
                url: cloudapiUrl
            });

            next();
        },

        function getCurrSubusers(ctx, next) {
            ctx.smartdcClient.listUsers(accUuid, function (err, currSubusers) {
                var currSubuserFromLogin = {};
                currSubusers.forEach(function (s) {
                    currSubuserFromLogin[s.login] = s;
                });

                ctx.subusers = [];
                ctx.subusersToCreate = [];
                ctx.subusersToDelete = [];

                opts.subusers.forEach(function (s) {
                    if (currSubuserFromLogin[s.login] === undefined) {
                        ctx.subusersToCreate.push(s);
                    } else {
                        ctx.subusers.push(currSubuserFromLogin[s.login]);
                    }
                });

                var wantSubuserLogins = opts.subusers.map(
                    function (s) { return s.login; });
                currSubusers.forEach(function (s) {
                    if (wantSubuserLogins.indexOf(s.login) === -1) {
                        ctx.subusersToDelete.push(s);
                    }
                });

                next(err);
            });
        },

        function deleteSubusers(ctx, next) {
            vasync.forEachPipeline({
                inputs: ctx.subusersToDelete,
                func: function delOneSubuser(subuser, nextSubuser) {
                    assert.uuid(subuser.id, 'subuser.id');
                    ctx.smardcClient.deleteUser(opts.account.login, subuser.id,
                        nextSubuser);
                }
            }, function (err) {
                next(err);
            });
        },

        function createSubusers(ctx, next) {
            vasync.forEachPipeline({
                inputs: ctx.subusersToCreate,
                func: function createOneSubuser(subuser, nextSubuser) {
                    assert.string(subuser.login, 'subuser.login');
                    assert.string(subuser.password, 'subuser.password');
                    assert.string(subuser.email, 'subuser.email');

                    t.comment(format('creating subuser %s/%s',
                        opts.account.login, subuser.login));
                    ctx.smartdcClient.createUser(subuser, function (err, s) {
                        if (err) {
                            nextSubuser(err);
                        } else {
                            ctx.madeAdditions = true;
                            ctx.subusers.push(s);
                            nextSubuser();
                        }
                    });
                }
            }, function finish(err) {
                next(err);
            });
        },

        // All subusers are given the pubKey of their owning account.
        function ensureSubusersHaveKey(ctx, next) {
            vasync.forEachPipeline({
                inputs: ctx.subusers,
                func: function ensureKeyOnOneSubuser(subuser, nextSubuser) {
                    ctx.smartdcClient.getUserKey(
                        opts.account.login,
                        subuser.id,
                        opts.account.fp,
                        function (err, key) {
                            if (err && err.name !== 'ResourceNotFoundError') {
                                // Some unexpected error.
                                nextSubuser(err);
                            } else if (err) {
                                t.comment(format('adding key to subuser %s/%s',
                                    opts.account.login, subuser.login));
                                ctx.smartdcClient.uploadUserKey(
                                    opts.account.login,
                                    subuser.id,
                                    {
                                        name: 'muskietest_key',
                                        key: opts.account.pubKey
                                    },
                                    function (uploadErr, _key) {
                                        nextSubuser(uploadErr);
                                    });
                            } else {
                                // Already have the key.
                                t.comment(format(
                                    'key is already on subuser %s/%s',
                                    opts.account.login,
                                    subuser.login));
                                nextSubuser();
                            }
                        });
                }
            }, function finish(err) {
                next(err);
            });
        },

        function getCurrPolicies(ctx, next) {
            ctx.smartdcClient.listPolicies(opts.account.login,
                                           function (err, currPolicies) {
                if (err) {
                    next(err);
                    return;
                }

                var currPolicyFromName = {};
                currPolicies.forEach(function (p) {
                    currPolicyFromName[p.name] = p;
                });

                ctx.policiesToCreate = [];
                ctx.policiesToDelete = [];

                opts.policies.forEach(function (p) {
                    if (currPolicyFromName[p.name] === undefined) {
                        ctx.policiesToCreate.push(p);
                    } else {
                        t.comment('already have policy ' + p.name);
                    }
                });

                var wantPolicyNames = opts.policies.map(
                    function (p) { return p.name; });
                currPolicies.forEach(function (p) {
                    if (wantPolicyNames.indexOf(p.name) === -1) {
                        ctx.policiesToDelete.push(p);
                    }
                });

                next();
            });
        },

        function deletePolicies(ctx, next) {
            vasync.forEachPipeline({
                inputs: ctx.policiesToDelete,
                func: function delOne(policy, nextPolicy) {
                    assert.uuid(policy.id, 'policy.id');
                    ctx.smardcClient.deletePolicy(opts.account.login, policy.id,
                        nextPolicy);
                }
            }, function (err) {
                next(err);
            });
        },

        function createPolicies(ctx, next) {
            vasync.forEachPipeline({
                inputs: ctx.policiesToCreate,
                func: function createOne(policy, nextPolicy) {
                    assert.string(policy.name, 'policy.name');
                    assert.arrayOfString(policy.rules, 'policy.rules');

                    t.comment('creating policy ' + policy.name);
                    ctx.smartdcClient.createPolicy(policy, function (err) {
                        ctx.madeAdditions = true;
                        nextPolicy(err);
                    });
                }
            }, function finish(err) {
                next(err);
            });
        },

        // If we make subuser or policy additions, then we need to *wait*
        // before adding roles, otherwise CloudAPI CreateRole errors, e.g.:
        //      Invalid subuser: muskietest_subuser
        //
        // This is because CloudAPI CreateRole is using *mahi* to get current
        // subuser and policy information. Mahi is a cache that can be
        // 10s (the mahi.git poll interval) out of date.
        function lamePauseForCloudapi(ctx, next) {
            if (!ctx.madeAdditions) {
                next();
                return;
            }

            const MAHI_POLL_INTERVAL_S = 10;
            t.comment('waiting ' + MAHI_POLL_INTERVAL_S + ' for mahi to sync ' +
                'subuser and policy additions so CloudAPI CreateRole does ' +
                'not choke');
            setTimeout(next, MAHI_POLL_INTERVAL_S * 1000);
        },

        function getCurrRoles(ctx, next) {
            ctx.smartdcClient.listRoles(opts.account.login,
                                        function (err, currRoles) {
                if (err) {
                    next(err);
                    return;
                }

                var currRoleFromName = {};
                currRoles.forEach(function (r) {
                    currRoleFromName[r.name] = r;
                });

                ctx.rolesToCreate = [];
                ctx.rolesToDelete = [];

                opts.roles.forEach(function (r) {
                    if (currRoleFromName[r.name] === undefined) {
                        ctx.rolesToCreate.push(r);
                    } else {
                        t.comment('already have role ' + r.name);
                    }
                });

                var wantRoleNames = opts.roles.map(
                    function (r) { return r.name; });
                currRoles.forEach(function (r) {
                    if (wantRoleNames.indexOf(r.name) === -1) {
                        ctx.rolesToDelete.push(r);
                    }
                });

                next();
            });
        },

        function deleteRoles(ctx, next) {
            vasync.forEachPipeline({
                inputs: ctx.rolesToDelete,
                func: function delOne(role, nextRole) {
                    assert.uuid(role.id, 'role.id');
                    ctx.smardcClient.deleteRole(opts.account.login, role.id,
                        nextRole);
                }
            }, function (err) {
                next(err);
            });
        },

        function createRoles(ctx, next) {
            vasync.forEachPipeline({
                inputs: ctx.rolesToCreate,
                func: function createOne(role, nextRole) {
                    assert.string(role.name, 'role.name');
                    assert.optionalArrayOfObject(role.members, 'role.members');
                    assert.optionalArrayOfString(role.default_members,
                        'role.default_members');
                    assert.optionalArray(role.policies,
                        'role.policies');

                    t.comment('creating role ' + role.name);
                    ctx.smartdcClient.createRole(role, function (err) {
                        ctx.madeRoleAdditions = true;
                        nextRole(err);
                    });
                }
            }, function finish(err) {
                next(err);
            });
        },

        // If we made role additions, then we need to *wait* before using
        // these in a test case, because Manta uses its "authcache" (aka mahi)
        // service for RBAC info and that service can be 10s out of date.
        // (We've already waited above for subuser and/or policy additions.)
        function pauseForAuthcache(ctx, next) {
            if (!ctx.madeRoleAdditions) {
                next();
                return;
            }

            const MAHI_POLL_INTERVAL_S = 10;
            t.comment('waiting ' + MAHI_POLL_INTERVAL_S + 's for authcache ' +
                'to sync role additions so Muskie auth is up to date');
            setTimeout(next, MAHI_POLL_INTERVAL_S * 1000);
        }
    ]}, function finishAll(err) {
        if (context.smartdcClient) {
            context.smartdcClient.client.close();
        }
        cb(err);
    });
}

function _ensureAccount(opts, cb) {
    assert.object(opts.t, 'opts.t');
    assert.object(opts.ufdsClient, 'opts.ufdsClient');
    assert.bool(opts.isOperator, 'opts.isOperator');
    assert.string(opts.loginPrefix, 'opts.loginPrefix');
    assert.string(opts.cacheDir, 'opts.cacheDir');

    var t = opts.t;
    var info;

    vasync.pipeline({arg: {}, funcs: [
        function findExistingKeys(ctx, next) {
            var pat = path.join(opts.cacheDir, opts.loginPrefix + '*.id_rsa*');
            glob(pat, function (err, files) {
                if (err) {
                    next(err);
                } else if (files.length === 0) {
                    // No existing key in the cache: we'll create a new account.
                    // Note: It is possible there is a pre-existing account
                    // and they we've just lost its key. We could theoretically
                    // add a new key and re-use that account. However, we'll
                    // end up having to wait for 5 minutes for muskie's
                    // client-side mahi cache to clear. A new account avoids
                    // that cache delay.
                    ctx.privKeyPath = null;
                    ctx.login = opts.loginPrefix + uuidv4().split('-')[0];
                    next();
                } else if (files.length === 2) {
                    // Two files mean there is an existing key:
                    //      $cacheDir/$login.id_rsa     # the private key
                    //      $cacheDir/$login.id_rsa.pub # the public key
                    // If that login still exists, we'll re-use it.
                    ctx.privKeyPath = files
                        .filter(
                            function (f) { return f.endsWith('.id_rsa'); })[0];
                    assert(ctx.privKeyPath.endsWith('.id_rsa'));
                    ctx.login = path.basename(ctx.privKeyPath)
                        .slice(0, -('.id_rsa'.length));
                    t.comment('found existing key for login "' +
                        ctx.login + '"');
                    next();
                } else {
                    next(new VError(
                        'unexpected number of files matching "%s": %s',
                        pat, files.join(', ')));
                }
            });
        },

        function ensureTheAccount(ctx, next) {
            opts.ufdsClient.getUserEx({
                searchType: 'login',
                value: ctx.login
            }, function (getErr, account) {
                if (getErr && getErr.name !== 'ResourceNotFoundError') {
                    next(new VError(getErr,
                        'unexpected error loading account "%s"', ctx.login));
                } else if (account) {
                    ctx.account = account;
                    t.comment('already have account "' + ctx.login + '"');
                    next();
                } else {
                    opts.ufdsClient.addUser({
                        login: ctx.login,
                        email: ctx.login + '@localhost',
                        userpassword: uuidv4(),
                        approved_for_provisioning: true
                    }, function (addErr, newAccount) {
                        if (addErr) {
                            next(new VError(addErr,
                                'could not create account "%s"', ctx.login));
                        } else {
                            t.comment('created account "' + ctx.login + '"');
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
            if (!ctx.privKeyPath) {
                ctx.privKeyPath = path.join(opts.cacheDir,
                    ctx.login + '.id_rsa');
                var argv = [
                    'ssh-keygen',
                    '-t', 'rsa',
                    '-C', ctx.login,
                    '-b', '2048',
                    '-N', '',
                    '-f', ctx.privKeyPath
                ];
                forkExecWait({
                    argv: argv,
                    includeStderr: true
                }, function (err, _info) {
                    if (err) {
                        next(new VError(err,
                            'failed to generate key for login "%s"',
                            ctx.login));
                    } else {
                        t.comment('created new key "' + ctx.privKeyPath + '"');
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
            var keyInfo = {
                openssh: ctx.pubKey,
                name: 'muskietest_key'
            };
            ctx.fp = getKeyFingerprint(ctx.pubKey);
            opts.ufdsClient.getKey(ctx.account, keyInfo.name,
                                   function (getErr, key) {
                if (getErr && getErr.name !== 'ResourceNotFoundError') {
                    next(new VError(getErr,
                        'unexpected error checking for key on account "%s"',
                        ctx.login));
                } else if (getErr) {
                    opts.ufdsClient.addKey(ctx.account, keyInfo,
                                           function (addErr) {
                        if (addErr) {
                            next(new VError(addErr,
                                'could not add key to account "%s"',
                                ctx.login));
                        } else {
                            t.comment('added key "' + ctx.fp + '" to account');
                            next();
                        }
                    });
                } else if (key.fingerprint !== ctx.fp) {
                    next(new VError(
                        'expected fingerprint "%s" for existing "%s" key ' +
                            'on account "%s", got "%s"',
                        ctx.fp, keyInfo.name, ctx.login, key.fingerprint));
                } else {
                    next();
                }
            });
        },

        function buildInfo(ctx, next) {
            info = {
                login: ctx.login,
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
            return (!!lastErr);
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
                t.comment(format('[%s] HEAD %s -> %s',
                    new Date().toISOString(),
                    p,
                    res ? res.statusCode : '<no response>'));
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
        });
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
// - muskietest_account_$randomHexChars
// - muskietest_operator_$randomHexChars
function ensureTestAccounts(t, cb) {
    var context = {
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
                fs.mkdir(DB_DIR, function onMkdir(err) {
                    if (err && err.code !== 'EEXIST') {
                        next(err);
                    } else {
                        next();
                    }
                });
            },
            function getLock(ctx, next) {
                qlocker.lock(ACCOUNTS_LOCK_FILE, function (err, unlockFn) {
                    t.comment(format('[%s] acquired test accounts lock',
                        new Date().toISOString()));
                    ctx.unlockFn = unlockFn;
                    next(err);
                });
            },

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
                _ensureAccount({
                    t: t,
                    ufdsClient: ctx.ufdsClient,
                    loginPrefix: 'muskietest_account_',
                    isOperator: false,
                    cacheDir: DB_DIR
                }, function (err, info) {
                    if (err) {
                        next(err);
                    } else {
                        t.ok(info, 'ensured regular account "' +
                            info.login + '"');
                        ctx.accounts.regular = info;
                        next();
                    }
                });
            },

            function ensureRegularAccountRbac(ctx, next) {
                _ensureRbacSettings({
                    t: t,
                    account: ctx.accounts.regular,
                    subusers: [
                        {
                            login: 'muskietest_subuser',
                            password: 'secret123',
                            email: ctx.accounts.regular.login +
                                '_subuser@localhost'
                        }
                    ],
                    policies: [
                        {
                            name: 'muskietest_policy_read',
                            rules: [
                                'Can getobject'
                            ]
                        },
                        {
                            name: 'muskietest_policy_write',
                            rules: [
                                'Can putobject',
                                'Can putdirectory'
                            ]
                        },
                        {
                            name: 'muskietest_policy_star',
                            rules: [
                                'Can getobject *',
                                'Can putobject'
                            ]
                        },
                        {
                            name: 'muskietest_policy_glob',
                            rules: [
                                // "/:login/stor/test-ac-dir-*" is the test dir
                                // in which all test objects in this test file
                                // are placed.
                                'Can getobject /' + ctx.accounts.regular.login +
                                    '/stor/test-ac-dir-*/globbity-*'
                            ]
                        }
                    ],
                    roles: [
                        {
                            name: 'muskietest_role_default',
                            members: [
                                {
                                    type: 'subuser',
                                    login: 'muskietest_subuser',
                                    default: true
                                }
                            ],
                            policies: [ { 'name': 'muskietest_policy_read' } ]
                        },
                        {
                            name: 'muskietest_role_limit',
                            members: [
                                { type: 'subuser', login: 'muskietest_subuser' }
                            ],
                            policies: [ { 'name': 'muskietest_policy_read' } ]
                        },
                        {
                            name: 'muskietest_role_other',
                            policies: [ { 'name': 'muskietest_policy_read' } ]
                        },
                        {
                            name: 'muskietest_role_write',
                            members: [
                                { type: 'subuser', login: 'muskietest_subuser' }
                            ],
                            policies: [ { 'name': 'muskietest_policy_write' } ]
                        },
                        {
                            name: 'muskietest_role_star',
                            members: [
                                { type: 'subuser', login: 'muskietest_subuser' }
                            ],
                            policies: [ { 'name': 'muskietest_policy_star' } ]
                        },
                        {
                            name: 'muskietest_role_glob',
                            members: [
                                { type: 'subuser', login: 'muskietest_subuser' }
                            ],
                            policies: [ { 'name': 'muskietest_policy_glob' } ]
                        },
                        {
                            name: 'muskietest_role_all',
                            members: [
                                { type: 'subuser', login: 'muskietest_subuser' }
                            ],
                            policies: [
                                { 'name': 'muskietest_policy_read' },
                                { 'name': 'muskietest_policy_write' }
                            ]
                        }
                    ]
                }, function (err) {
                    t.ifError(err, 'ensured RBAC settings for account ' +
                        ctx.accounts.regular.login);
                    next(err);
                });
            },

            // Create (or load) the 'muskietest_operator_...' account.
            function ensureOperatorAccount(ctx, next) {
                _ensureAccount({
                    t: t,
                    ufdsClient: ctx.ufdsClient,
                    loginPrefix: 'muskietest_operator_',
                    isOperator: true,
                    cacheDir: DB_DIR
                }, function (err, info) {
                    if (err) {
                        next(err);
                    } else {
                        t.ok(info, 'ensured operator account "' +
                            info.login + '"');
                        ctx.accounts.operator = info;
                        next();
                    }
                });
            },

            function ensureOperatorAccountRbac(ctx, next) {
                _ensureRbacSettings({
                    t: t,
                    account: ctx.accounts.operator,
                    subusers: [],
                    policies: [
                        {
                            name: 'muskietest_policy_read',
                            rules: [ 'can getobject', 'can listdirectory' ]
                        }
                    ],
                    roles: [
                        {
                            name: 'muskietest_role_xacct',
                            members: [
                                {
                                    type: 'account',
                                    login: ctx.accounts.regular.login
                                }
                            ],
                            policies: [
                                { name: 'muskietest_policy_read' }
                            ]
                        }
                    ]
                }, function (err) {
                    t.ifError(err, 'ensured RBAC settings for account ' +
                        ctx.accounts.operator.login);
                    next(err);
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
            }
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
                    t.comment(format('[%s] releasing test accounts lock',
                        new Date().toISOString()));
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
            }
        ]}, function (cleanupErr) {
            t.ifError(cleanupErr, 'no error cleaning up ensureTestAccounts');
            cb(err, context.accounts);
        });
    });
}


// Return the MPU upload path for a given upload ID.
// Optionally also append a part number, if given.
function mpuUploadPath(accountLogin, uploadId, partNum) {
    assert.string(accountLogin, 'accountLogin');
    assert.uuid(uploadId, 'uploadId');
    assert.optionalFinite(partNum, 'partNum');

    var c;
    var len;
    var p;
    var prefix;

    c = uploadId.charAt(uploadId.length - 1);
    len = jsprim.parseInteger(c, { base: 16 });
    assert(!isNaN(len) && len >= 1 && len <= 4,
        'invalid prefix length: ' + len);
    prefix = uploadId.substring(0, len);
    p = '/' + accountLogin + '/uploads/' + prefix + '/' + uploadId;

    if (typeof (partNum) === 'number') {
        p += '/' + partNum;
    }

    return (p);
}



///--- Exports

module.exports = {
    assertMantaRes: assertMantaRes,
    ensureTestAccounts: ensureTestAccounts,
    mantaClientFromAccountInfo: mantaClientFromAccountInfo,
    mantaClientFromSubuserInfo: mantaClientFromSubuserInfo,
    signReq: signReq,

    createJsonClient: createJsonClient,
    createStringClient: createStringClient,
    createLogger: createLogger,
    getKeyFingerprint: getKeyFingerprint,

    mpuUploadPath: mpuUploadPath
};
