/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

// Tests for (possibly long) timeouts. These are separate from obj.test.js
// because they can take a lot of idle time to run, which gets in the way
// of quick edit/test development cycles.

var MemoryStream = require('stream').PassThrough;
var test = require('tap').test;
var uuidv4 = require('uuid/v4');

var helper = require('../helper');



///--- Globals

var assertMantaRes = helper.assertMantaRes;
var client;
var testAccount;
var testDir;
var TEXT = 'The lazy brown fox \nsomething \nsomething foo';


///--- Tests

test('setup: test account', function (t) {
    helper.ensureTestAccounts(t, function (err, accounts) {
        t.ifError(err, 'no error loading/creating test accounts');
        testAccount = accounts.regular;
        t.ok(testAccount, 'have regular test account: ' + testAccount.login);
        t.end();
    });
});

test('setup: test dir', function (t) {
    client = helper.mantaClientFromAccountInfo(testAccount);
    testDir = '/' + testAccount.login + '/stor/test-obj-timeout-' +
        uuidv4().split('-')[0];

    client.mkdir(testDir, function (err) {
        t.ifError(err, 'no error making testDir:' + testDir);
        t.end();
    });
});


// The default for this timeout is 45s.
test('put timeout', function (t) {
    var key = testDir + '/put-timeout';
    var opts = {
        size: Buffer.byteLength(TEXT),
        type: 'text/plain'
    };
    var stream = new MemoryStream();

    client.put(key, stream, opts, function (err, res) {
        t.ok(err);
        t.equal(err.name, 'UploadTimeoutError');
        t.end();
    });

    setImmediate(function () {
        stream.write(TEXT.substr(0, 1));
    });
});


test('teardown', function (t) {
    client.rmr(testDir, function onRm(err) {
        t.ifError(err, 'remove testDir: ' + testDir);
        t.end();
    });
});
