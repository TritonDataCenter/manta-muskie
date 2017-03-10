/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
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



///--- Globals

http.globalAgent.maxSockets = 50;
https.globalAgent.maxSockets = 50;

var TOKEN_CFG = {
    salt: process.env.MUSKIE_SALT,
    key: process.env.MUSKIE_KEY,
    iv: process.env.MUSKIE_IV,
    maxAge: +process.env.MUSKIE_MAX_AGE || 604800000
};

var POSEIDON_ID = process.env.MUSKIE_POSEIDON_ID ||
        '930896af-bf8c-48d4-885c-6573a94b1853';


///--- Helpers

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
    var key = fs.readFileSync(process.env.HOME + '/.ssh/id_rsa', 'utf8');
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
        subuser: process.env.MANTA_SUBUSER,
        url: process.env.MANTA_URL || 'http://localhost:8080',
        user: process.env.MANTA_USER || 'admin'
    });

    return (client);
}


function createUserClient(login) {
    var key = fs.readFileSync(process.env.HOME + '/.ssh/id_rsa', 'utf8');
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
    var key = fs.readFileSync(process.env.HOME + '/.ssh/id_rsa', 'utf8');
    var log = createLogger();
    var client = smartdc.createClient({
        log: log,
        sign: smartdc.privateKeySigner({
            key: key,
            keyId: process.env.SDC_KEY_ID,
            user: process.env.SDC_ACCOUNT
        }),
        user: process.env.SDC_ACCOUNT || 'admin',
        url: process.env.SDC_URL || 'http://localhost:8080'
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
    var key = fs.readFileSync(process.env.HOME + '/.ssh/id_rsa', 'utf8');
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



///--- Exports

module.exports = {

    after: function after(teardown) {
        module.parent.exports.tearDown = function _teardown(cb) {
            cb = once(cb);
            var d = domain.create();
            var self = this;

            d.once('error', function (e) {
                console.error('after:\n' + e.stack);
                process.exit(1);
            });
            d.run(function () {
                teardown.call(self, function (err) {
                    if (err) {
                        console.error('after:\n' + err.stack);
                        process.exit(1);
                    }
                    cb();
                });
            });
        };
    },

    before: function before(setup) {
        module.parent.exports.setUp = function _setup(cb) {
            cb = once(cb);

            var d = domain.create();
            var self = this;

            d.once('error', function (e) {
                console.error('before:\n' + e.stack);
                process.exit(1);
            });

            d.run(function () {
                setup.call(self, function (err) {
                    if (err) {
                        console.error('before:\n' + err.stack);
                        process.exit(1);
                    }
                    cb();
                });
            });
        };
    },

    test: function test(name, tester) {
        module.parent.exports[name] = function _(t) {
            var self = this;
            var d = domain.create();
            d.once('error', function (e) {
                console.error(name + ':\n' + e.stack);
                process.exit(1);
            });
            d.run(function () {
                var _done = false;
                t.end = once(function end() {
                    if (!_done) {
                        _done = true;
                        t.done();
                    }
                });
                t.notOk = function notOk(ok, message) {
                    return (t.ok(!ok, message));
                };
                t.checkResponse = checkResponse.bind(self, t);
                tester.call(self, t);
            });
        };
    },

    POSEIDON_ID: POSEIDON_ID,
    createClient: createClient,
    createJsonClient: createJsonClient,
    createRawClient: createRawClient,
    createUserClient: createUserClient,
    createSDCClient: createSDCClient,
    createLogger: createLogger,
    createAuthToken: createAuthToken,
    signUrl: signUrl
};
