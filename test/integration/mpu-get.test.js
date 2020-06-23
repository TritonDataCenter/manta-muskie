/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var test = require('tap').test;
var uuidv4 = require('uuid/v4');
var VError = require('verror');

var helper = require('../helper');


///--- Globals

var assertMantaRes = helper.assertMantaRes;
var mpuUploadPath = helper.mpuUploadPath;

var enableMPU = Boolean(require('../../etc/config.json').enableMPU);
var testOpts = {
    skip: !enableMPU && 'MPU is not enabled (enableMPU in config)'
};


///--- Tests

test('mpu get', testOpts, function (suite) {
    var client;
    var testAccount;
    var testOperAccount;

    suite.test('setup: test account', function (t) {
        helper.ensureTestAccounts(t, function (err, accounts) {
            t.ifError(err, 'no error loading/creating test accounts');
            testAccount = accounts.regular;
            t.ok(testAccount, 'have regular test account: ' +
                testAccount.login);
            testOperAccount = accounts.operator;
            t.ok(testOperAccount,
                'have operator test account: ' + testOperAccount.login);
            client = helper.mantaClientFromAccountInfo(testAccount);
            t.end();
        });
    });

    // Get: bad input (happy path tested in create)
    suite.test('get upload: non-uuid id', function (t) {
        var bogus = 'foobar';
        var action = 'state';
        var p = '/' + client.user + '/uploads/0/' + bogus + '/' + action;
        var options = {
            headers: {
                'content-type': 'application/json',
                'accept': 'application/json'
            }
        };

        client.signRequest({
            headers: options.headers
        }, function (err) {
            t.ifError(err, 'expect no error in signRequest');
            if (err) {
                t.end();
                return;
            }

            client.get(p, options, function (err2, _, res) {
                t.ok(err2);
                if (!err2) {
                    t.end();
                    return;
                }

                assertMantaRes(t, res, 404);
                t.ok(VError.hasCauseWithName(err2, 'ResourceNotFoundError'));
                t.end();
            });
        });
    });

    suite.test('get upload: non-existent id', function (t) {
        var noSuchUploadId = uuidv4();
        // Force the trailing char to '1', for a valid uploads path prefix len.
        // Otherwise `mpuUploadPath` will assert.
        noSuchUploadId = noSuchUploadId.slice(0, -1) + '1';

        client.getUpload(noSuchUploadId, {
            account: client.user,
            partsDirectory: mpuUploadPath(client.user, noSuchUploadId)
        }, function (err, upload) {
            t.ok(err);
            t.ok(VError.hasCauseWithName(err, 'ResourceNotFoundError'),
                'error chain includes ResourceNotFoundError, got: ' + err);
            t.end();
        });
    });

    suite.end();
});
