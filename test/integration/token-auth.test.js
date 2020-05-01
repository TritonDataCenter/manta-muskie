/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

// Testing webapi's *token* auth. See "auth.test.js" for other auth tests.

var fs = require('fs');
var util = require('util');

var test = require('tap').test;
var uuidv4 = require('uuid/v4');

var auth = require('../../lib/auth');
var helper = require('../helper');


///--- Tests

test('token auth', function (suite) {
    var client;
    var dir;
    var jsonClient = helper.createJsonClient();
    var key;
    var stringClient = helper.createStringClient();
    var testAccount;
    var testOperatorAccount;

    suite.test('setup: test accounts', function (t) {
        helper.ensureTestAccounts(t, function (err, accounts) {
            t.ifError(err, 'no error loading/creating test accounts');
            testAccount = accounts.regular;
            testOperatorAccount = accounts.operator;
            t.ok(testAccount, 'have regular test account: ' +
                testAccount.login);
            t.ok(testOperatorAccount, 'have operator test account: ' +
                testOperatorAccount.login);
            t.end();
        });
    });

    suite.test('setup: test dir', function (t) {
        client = helper.mantaClientFromAccountInfo(testAccount);
        var root = '/' + client.user + '/stor';
        dir = root + '/test-tokenauth-dir-' + uuidv4().split('-')[0];

        client.mkdir(dir, function (err) {
            t.ifError(err, 'no error making test dir ' + dir);
            t.end();
        });
    });

    suite.test('fail auth with bogus token crypto config', function (t) {
        var tokenOpts = {
            caller: {
                account: {
                    uuid: testAccount.uuid
                }
            }
        };
        var aes = {
            salt: 'AAAAAAAAAAAAAAAA',
            key: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
            iv: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
            maxAge: 604800000
        };

        auth.createAuthToken(tokenOpts, aes, function (tokenErr, token) {
            if (tokenErr || !token) {
                t.ifError(tokenErr || new Error('no token'));
                t.end();
                return;
            }

            jsonClient.get({
                path: dir,
                headers: {
                    authorization: 'Token ' + token
                }
            }, function (err, res) {
                t.ok(err, 'got error attempting to auth with bad token');
                if (err) {
                    t.equal(err.statusCode, 403);
                    t.equal(err.restCode, 'InvalidAuthenticationToken');
                }
                t.end();
            });
        });
    });


    suite.test('create and auth with operator token', function (t) {
        var tokenOpts = {
            caller: {
                account: {
                    uuid: testOperatorAccount.uuid
                }
            }
        };

        // The crypto values Muskie will use to decrypt auth token are in the
        // Muskie config. Use those same ones for creating a valid auth token.
        var muskieConfig = JSON.parse(
            fs.readFileSync('/opt/smartdc/muskie/etc/config.json'));
        t.ok(muskieConfig.authToken.key,
            'have "authToken.key" in muskie config file')

        auth.createAuthToken(tokenOpts, muskieConfig.authToken,
                             function (err, token) {
            t.ifError(err);
            if (err) {
                t.end();
                return;
            }

            var opts = {
                path: `/${testOperatorAccount.login}/stor`,
                headers: {
                    authorization: 'Token ' + token
                }
            };
            stringClient.get(opts, function (err, _req, res) {
                t.ifError(err, 'no error authenticating with created token');
                t.equal(res.statusCode, 200, '200 status code');
                t.end();
            });
        });
    });


    suite.test('create auth token, signURL with method', function (t) {
        var opts = {
        };

        client.signURL({
            method: 'POST',
            path: `/${testAccount.login}/tokens`
        }, function (signErr, path) {
            t.ifError(signErr);
            t.ok(path)

            jsonClient.post(path, null, function (postErr, _req, res, obj) {
                t.ifError(postErr);
                t.equal(res.statusCode, 201, '201 response status');
                t.ok(obj);
                t.ok(obj.token, 'got a JSON response with a "token"');
                t.end();
            });
        });
    });

    suite.test('teardown', function (t) {
        if (client) {
            client.close();
        }

        client.rmr(dir, function onRm(err) {
            t.ifError(err, 'remove test dir ' + dir);
            t.end();
        });
    });

    suite.end();
});
