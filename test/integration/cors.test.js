/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var crypto = require('crypto');

var MemoryStream = require('stream').PassThrough;
var test = require('tap').test;
var uuidv4 = require('uuid/v4');

var helper = require('../helper');


///--- Globals

var TEXT = 'hello world';


///--- Tests

test('cors', function (suite) {
    var client;
    var dir;
    var key;
    var testAccount;

    // Call "GET $key" with given headers, test for successful response and
    // call back with the response headers `function (null, resHeaders)`.
    // The first callback arg is always empty (null or undefined).
    function testGet(t, headers, cb) {
        var opts = {
            headers: headers
        };
        client.get(key, opts, function (err, stream, res) {
            t.ifError(err, '"GET ' + key + '" did not error');
            if (err) {
                cb();
                return;
            }

            var body = '';
            stream.setEncoding('utf8');
            stream.on('data', function (chunk) {
                body += chunk;
            });
            stream.once('error', function (err2) {
                t.ifError(err2, 'no GET stream error');
                cb();
            });
            stream.once('end', function () {
                t.equal(res.statusCode, 200,
                    'got 200 status code: ' + res.statusCode);
                t.equal(body, TEXT, 'GET body matches TEXT');
                cb(null, res.headers);
            });
        });
    }

    // Call "PUT $key" with the given headers, test for successful response
    // and callback with `function ()`, i.e. no error is returned.
    function testPut(t, headers, cb) {
        var md5 = crypto.createHash('md5');
        var opts = {
            headers: headers,
            md5: md5.update(TEXT).digest('base64'),
            size: Buffer.byteLength(TEXT),
            type: 'text/plain'
        };
        var stream = new MemoryStream();

        client.put(key, stream, opts, function onPut(err) {
            t.ifError(err, '"PUT ' + key + '" did not error');
            cb();
        });

        setImmediate(function streamInContent() {
            stream.end(TEXT);
        });
    }

    suite.test('setup: test account', function (t) {
        helper.ensureTestAccounts(t, function (err, accounts) {
            t.ifError(err, 'no error loading/creating test accounts');
            t.ok(accounts.regular, 'have regular test account: ' +
                accounts.regular.login);
            testAccount = accounts.regular;
            t.end();
        });
    });

    suite.test('setup: client and test dir', function (t) {
        client = helper.mantaClientFromAccountInfo(testAccount);
        var root = '/' + client.user + '/stor';
        dir = root + '/test-cors-dir-' + uuidv4().split('-')[0];
        key = dir + '/test-cors-file-' + uuidv4().split('-')[0];

        client.mkdir(dir, function (err) {
            t.ifError(err, 'make test dir ' + dir);
            t.end();
        });
    });

    suite.test('origin *', function (t) {
        var putHeaders = {
            'access-control-allow-origin': '*'
        };

        testPut(t, putHeaders, function () {
            var headers = {
                origin: 'http://127.0.0.1'
            };
            testGet(t, headers, function (_, resHeaders) {
                t.equal(resHeaders['access-control-allow-origin'],
                    headers.origin,
                    'access-control-allow-origin matches given Origin header');
                t.end();
            });
        });
    });

    suite.test('origin list of URLs', function (t) {
        var putHeaders = {
            'access-control-allow-origin': 'http://foo.com, http://bar.com'
        };

        testPut(t, putHeaders, function () {
            var headers = {
                origin: 'http://foo.com'
            };
            testGet(t, headers, function (_, resHeaders) {
                t.equal(resHeaders['access-control-allow-origin'],
                    headers.origin,
                    'access-control-allow-origin matches given Origin header');
                t.end();
            });
        });
    });

    suite.test('origin deny', function (t) {
        var putHeaders = {
            'access-control-allow-origin': 'http://foo.com'
        };

        testPut(t, putHeaders, function () {
            var headers = {
                origin: 'http://bar.com'
            };
            testGet(t, headers, function (_, resHeaders) {
                t.notOk(resHeaders['access-control-allow-origin'],
                    'there is no access-control-allow-origin header');
                t.end();
            });
        });
    });

    suite.test('method explicit', function (t) {
        var putHeaders = {
            'access-control-allow-origin': '*',
            'access-control-allow-methods': 'GET'
        };

        testPut(t, putHeaders, function () {
            var headers = {
                origin: 'http://foo.com'
            };
            testGet(t, headers, function (_, resHeaders) {
                t.equal(resHeaders['access-control-allow-origin'],
                    headers.origin,
                    'access-control-allow-origin matches given Origin header');
                t.end();
            });
        });
    });

    suite.test('method explicit fail', function (t) {
        var putHeaders = {
            'access-control-allow-origin': '*',
            'access-control-allow-methods': 'DELETE'
        };

        testPut(t, putHeaders, function () {
            var headers = {
                origin: 'http://foo.com'
            };
            testGet(t, headers, function (_, resHeaders) {
                t.notOk(resHeaders['access-control-allow-origin']);
                t.notOk(resHeaders['access-control-allow-methods']);
                t.end();
            });
        });
    });

    suite.test('other access-control ok', function (t) {
        var putHeaders = {
            'access-control-allow-origin': '*',
            'access-control-expose-headers': 'x-foo',
            'access-control-max-age': 3600
        };

        testPut(t, putHeaders, function () {
            var headers = {
                origin: 'http://foo.com'
            };
            testGet(t, headers, function (_, resHeaders) {
                t.equal(resHeaders['access-control-allow-origin'],
                    headers.origin);
                t.equal(resHeaders['access-control-expose-headers'],
                    putHeaders['access-control-expose-headers']);
                t.notOk(resHeaders['access-control-max-age']);
                t.end();
            });
        });
    });

    suite.test('teardown', function (t) {
        client.rmr(dir, function onRm(err) {
            t.ifError(err, 'remove test dir ' + dir);
            t.end();
        });
    });

    suite.end();
});
