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

var assert = require('assert-plus');
var bunyan = require('bunyan');
var manta = require('manta');
var restifyClients = require('restify-clients');
var smartdc = require('smartdc');
var smartdc_auth = require('smartdc-auth');
var sshpk = require('sshpk');

var auth = require('../lib/auth');


///--- Globals

// XXX can we drop these?
http.globalAgent.maxSockets = 50;
https.globalAgent.maxSockets = 50;


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

function createLogger(name, stream) {
    var log = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'warn'),
        name: name || process.argv[1],
        stream: stream || process.stdout,
        src: true,
        serializers: restifyClients.bunyan.serializers
    });
    return (log);
}


function createClient() {
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
            user: process.env.MANTA_USER
        }),
        rejectUnauthorized: false,
        url: process.env.MANTA_URL,
        user: process.env.MANTA_USER
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


function createRawClient() {
    assert.string(process.env.MANTA_URL, 'process.env.MANTA_URL');

    var log = createLogger();
    var client = restifyClients.createClient({
        agent: false,
        connectTimeout: 250,
        log: log,
        rejectUnauthorized: false,
        retry: false,
        type: 'http',
        url: process.env.MANTA_URL
    });

    return (client);
}


function createSDCClient() {
    assert.string(process.env.SDC_URL, 'process.env.SDC_URL');
    assert.string(process.env.SDC_ACCOUNT, 'process.env.SDC_ACCOUNT');
    assert.string(process.env.SDC_KEY_ID, 'process.env.SDC_KEY_ID');

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
        user: process.env.SDC_ACCOUNT,
        url: process.env.SDC_URL
    });

    return (client);
}

function createOperatorSDCClient() {
    assert.string(process.env.SDC_URL, 'process.env.SDC_URL');

    var key = getOperatorPrivkey();
    var keyId = getKeyFingerprint(key);

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
        url: process.env.SDC_URL,
        user: TEST_OPERATOR
    });

    return (client);
}

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


function createAuthToken(opts, cb) {
    assert.string(process.env.MUSKIE_SALT, 'process.env.MUSKIE_SALT');
    assert.string(process.env.MUSKIE_KEY, 'process.env.MUSKIE_KEY');
    assert.string(process.env.MUSKIE_IV, 'process.env.MUSKIE_IV');
    assert.optionalString(process.env.MUSKIE_MAX_AGE, 'process.env.MUSKIE_MAX_AGE');

    var tokenCfg = {
        salt: process.env.MUSKIE_SALT,
        key: process.env.MUSKIE_KEY,
        iv: process.env.MUSKIE_IV,
        maxAge: +process.env.MUSKIE_MAX_AGE || 604800000
    };

    var check = ['salt', 'key', 'iv'].every(function (env) {
        if (!tokenCfg[env]) {
            cb(new Error('MUSKIE_' + env.toUpperCase() + ' required'));
            return (false);
        } else {
            return (true);
        }
    });

    if (!check) {
        return;
    }

    auth.createAuthToken(opts, tokenCfg, function (err, token) {
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


function signRequest(opts, cb) {
    var key = opts.key || getRegularPrivkey();

    var sign = manta.privateKeySigner({
        key: key,
        keyId: opts.keyId || process.env.MANTA_KEY_ID,
        user: opts.user || process.env.MANTA_USER
    });

    var rs = smartdc_auth.requestSigner({
        sign: sign,
        mantaSubUser: true
    });

    var date = rs.writeDateHeader();

    rs.sign(function gotSignature(err, authz) {
        if (err) {
            cb(err);
            return;
        }
        cb(null, authz, date);
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
        query: opts.query,
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


///--- Exports

module.exports = {
    POSEIDON_ID: POSEIDON_ID,
    TEST_OPERATOR: TEST_OPERATOR,
    createClient: createClient,
    createJsonClient: createJsonClient,
    createRawClient: createRawClient,
    createUserClient: createUserClient,
    createSDCClient: createSDCClient,
    createLogger: createLogger,
    createAuthToken: createAuthToken,
    createOperatorSDCClient: createOperatorSDCClient,
    createOperatorClient: createOperatorClient,
    signRequest: signRequest,
    signUrl: signUrl,
    getRegularPubkey: getRegularPubkey,
    getRegularPrivkey: getRegularPrivkey,
    getOperatorPubkey: getOperatorPubkey,
    getOperatorPrivkey: getOperatorPrivkey,
    getKeyFingerprint: getKeyFingerprint
};
