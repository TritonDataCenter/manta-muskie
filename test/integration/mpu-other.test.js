/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

// Other MPU-related tests.

var test = require('tap').test;

var helper = require('../helper');


///--- Globals

var assertMantaRes = helper.assertMantaRes;
var enableMPU = Boolean(require('../../etc/config.json').enableMPU);
var testOpts = {
    skip: !enableMPU && 'MPU is not enabled (enableMPU in config)'
};


///--- Tests

test('mpu other', testOpts, function (suite) {
    var client;
    var testAccount;
    var testOperAccount;

    suite.test('setup: test account', function (t) {
        helper.ensureTestAccounts(t, function (err, accounts) {
            t.ifError(err, 'no error loading/creating test accounts');
            testAccount = accounts.regular;
            t.ok(testAccount, 'have regular test account: ' + testAccount.login);
            testOperAccount = accounts.operator;
            t.ok(testOperAccount,
                'have operator test account: ' + testOperAccount.login);
            client = helper.mantaClientFromAccountInfo(testAccount);
            t.end();
        });
    });

    suite.test('rmdir /:login/uploads should fail', function (t) {
        var uploadsDir = '/' + client.user + '/uploads';

        client.unlink(uploadsDir, function (err, res) {
            t.ok(err);
            t.equal(err.name, 'OperationNotAllowedOnRootDirectoryError');
            assertMantaRes(t, res, 400);
            t.end();
        });
    });

    suite.end();
});
