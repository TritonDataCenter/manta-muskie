/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var domain = require('domain');
var http = require('http');
var https = require('https');

var auth = require('../lib/auth');

var bunyan = require('bunyan');
var fs = require('fs');
var manta = require('manta');
var once = require('once');
var restify = require('restify');
var smartdc = require('smartdc');
var sshpk = require('sshpk');
var VError = require('verror').VError;
var tape = require('tape');

///--- Globals

http.globalAgent.maxSockets = 50;
https.globalAgent.maxSockets = 50;

// Check the environment variables before we do anything else.
var envErr = checkEnvironment();
if (envErr) {
    console.error('Environment error: ' + envErr.message);
    process.exit(1);
}

var TOKEN_CFG = {
    salt: process.env.MUSKIE_SALT,
    key: process.env.MUSKIE_KEY,
    iv: process.env.MUSKIE_IV,
    maxAge: +process.env.MUSKIE_MAX_AGE || 604800000
};

var POSEIDON_ID = process.env.MUSKIE_POSEIDON_ID ||
        '930896af-bf8c-48d4-885c-6573a94b1853';

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
var TEST_OPERATOR_KEY = process.env.MUSKIETEST_OPERATOR_KEYFILE ||
        (process.env.HOME + '/.ssh/id_rsa_poseidon');


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

function createLogger(name, stream) {
    var log = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'warn'),
        name: name || process.argv[1],
        stream: stream || process.stdout,
        src: true,
        serializers: restify.bunyan.serializers
    });
    return (log);
}


function createClient() {
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
            user: process.env.MANTA_USER || 'admin'
        }),
        rejectUnauthorized: false,
        url: process.env.MANTA_URL || 'http://localhost:8080',
        user: process.env.MANTA_USER || 'admin'
    });

    return (client);
}


function createUserClient(login) {
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
            user: process.env.MANTA_USER || 'admin',
            subuser: login
        }),
        rejectUnauthorized: false,
        url: process.env.MANTA_URL || 'http://localhost:8080',
        user: process.env.MANTA_USER || 'admin',
        subuser: login
    });

    return (client);
}


function createJsonClient() {
    var log = createLogger();
    var client = restify.createClient({
        agent: false,
        connectTimeout: 250,
        log: log,
        rejectUnauthorized: false,
        retry: false,
        type: 'json',
        url: process.env.MANTA_URL || 'http://localhost:8080'
    });

    return (client);
}


function createRawClient() {
    var log = createLogger();
    var client = restify.createClient({
        agent: false,
        connectTimeout: 250,
        log: log,
        rejectUnauthorized: false,
        retry: false,
        type: 'http',
        url: process.env.MANTA_URL || 'http://localhost:8080'
    });

    return (client);
}


function createSDCClient() {
    var key = getRegularPrivkey();
    var log = createLogger();
    var client = smartdc.createClient({
        log: log,
        sign: smartdc.privateKeySigner({
            key: key,
            keyId: process.env.SDC_KEY_ID,
            user: process.env.SDC_ACCOUNT
        }),
        rejectUnauthorized: false,
        user: process.env.SDC_ACCOUNT || 'admin',
        url: process.env.SDC_URL || 'http://localhost:8080'
    });

    return (client);
}

function createOperatorSDCClient() {
    var key = getOperatorPrivkey();
    var keyId = sshpk.parseKey(key, 'auto').fingerprint('md5').toString();

    var log = createLogger();
    var client = smartdc.createClient({
        log: log,
        sign: smartdc.privateKeySigner({
            key: key,
            keyId: keyId,
            log: log,
            user: TEST_OPERATOR
        }),
        rejectUnauthorized: false,
        version: '9.0.0',
        url: process.env.SDC_URL || 'http://localhost:8080',
        user: TEST_OPERATOR
    });

    return (client);
}

function createOperatorClient() {
    var key = getOperatorPrivkey();
    var keyId = sshpk.parseKey(key, 'auto').fingerprint('md5').toString();

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
        url: process.env.MANTA_URL || 'http://localhost:8080',
        user: TEST_OPERATOR
    });

    return (client);
}

function checkResponse(t, res, code) {
    t.ok(res, 'null response');
    if (!res)
        return;
    t.equal(res.statusCode, code, 'HTTP status code mismatch');
    t.ok(res.headers);
    t.ok(res.headers.date);
    t.equal(res.headers.server, 'Manta');
    t.ok(res.headers['x-request-id']);
    t.ok(res.headers['x-server-name']);

    if (code === 200 || code === 201 || code === 202) {
        t.ok(res.headers['content-type']);
        var ct = res.headers['content-type'];
        /* JSSTYLED */
        if (!/application\/x-json-stream.*/.test(ct)) {
            t.ok(res.headers['content-length'] !== undefined);
            if (res.headers['content-length'] > 0)
                t.ok(res.headers['content-md5']);
        }
    }
}


function createAuthToken(opts, cb) {
    var check = ['salt', 'key', 'iv'].every(function (env) {
        if (!TOKEN_CFG[env]) {
            cb(new Error('MUSKIE_' + env.toUpperCase() + ' required'));
            return (false);
        } else {
            return (true);
        }
    });

    if (!check) {
        return;
    }

    auth.createAuthToken(opts, TOKEN_CFG, function (err, token) {
        if (err) {
            cb(err);
            return;
        } else if (!token) {
            cb(new Error('no token'));
            return;
        }
        cb(null, token);
    });
}



function signUrl(opts, expires, cb) {
    if (typeof (opts) === 'string') {
        opts = { path: opts };
    }
    if (typeof (expires) === 'function') {
        cb = expires;
        expires = Date.now() + (1000 * 300);
    }
    var key = getRegularPrivkey();
    var keyId = process.env.MANTA_KEY_ID;
    var url = process.env.MANTA_URL || 'http://localhost:8080';
    var user = process.env.MANTA_USER;
    var subuser = process.env.MANTA_SUBUSER;

    if (opts.client) {
        user = opts.client.user;
        subuser = opts.client.subuser;
    }

    manta.signUrl({
        algorithm: 'rsa-sha256',
        expires: expires,
        host: require('url').parse(url).host,
        keyId: keyId,
        method: opts.method || 'GET',
        path: opts.path,
        role: opts.role,
        'role-tag': opts['role-tag'],
        sign: manta.privateKeySigner({
            algorithm: 'rsa-sha256',
            key: key,
            keyId: keyId,
            log: createLogger(),
            user: user,
            subuser: subuser
        }),
        user: user,
        subuser: subuser
    }, cb);
}

/*
 * Loop through the required environment variables to make sure they are all
 * set. If one or more are not set, the names of the variables are combined
 * into an array and an error is returned.
 */
function checkEnvironment() {
    var environment = {
        'MANTA_URL': process.env.MANTA_URL,
        'MANTA_USER': process.env.MANTA_USER,
        'MANTA_KEY_ID': process.env.MANTA_KEY_ID,
        'MANTA_TLS_INSECURE': process.env.MANTA_TLS_INSECURE,
        'SDC_URL': process.env.SDC_URL,
        'SDC_ACCOUNT': process.env.SDC_ACCOUNT,
        'SDC_KEY_ID': process.env.SDC_KEY_ID,
        'SDC_TESTING': process.env.SDC_TESTING,
        'MUSKIE_IV': process.env.MUSKIE_IV,
        'MUSKIE_KEY': process.env.MUSKIE_KEY,
        'MUSKIE_SALT': process.env.MUSKIE_SALT
    };

    var unset = [];
    Object.keys(environment).forEach(function (key)  {
        if (typeof (environment[key]) !== 'string') {
            unset.push(key);
        }
    });
    if (unset.length > 0) {
        var errString = unset.join(', ');
        return (new VError('Environment variables ' + errString +
            ' must be set'));
    }
}

///--- Exports

module.exports = {

    afterEach: function afterEach(test, handler) {
        return function tapish(name, listener) {
            test(name, function (assert) {
                var _end = assert.end
                assert.end = function () {
                    assert.end = _end
                    handler(assert)
                }

                listener(assert)
            })
        }
    },

    beforeEach: function beforeEach(test, handler) {
        return function tapish(name, listener) {
            test(name, function (assert) {
                var _end = assert.end
                assert.end = function () {
                    assert.end = _end
                    listener(assert)
                }

                handler(assert)
            })
        }
    },

    test: function test(name, tester) {
        var self = this;
        var d = domain.create();
        d.once('error', function (e) {
            console.error(name + ':\n' + e.stack);
            process.exit(1);
        });
        d.run(function () {
            tape(name, tester);
        });
    },


    POSEIDON_ID: POSEIDON_ID,
    createClient: createClient,
    createJsonClient: createJsonClient,
    createRawClient: createRawClient,
    createUserClient: createUserClient,
    createSDCClient: createSDCClient,
    createLogger: createLogger,
    createAuthToken: createAuthToken,
    createOperatorSDCClient: createOperatorSDCClient,
    createOperatorClient: createOperatorClient,
    signUrl: signUrl,
    getRegularPubkey: getRegularPubkey,
    getRegularPrivkey: getRegularPrivkey
};
