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
var vasync = require('vasync');
var VError = require('verror');

var helper = require('../helper.js');


var enableMPU = Boolean(require('../../etc/config.json').enableMPU);
var testOpts = {
    skip: !enableMPU && 'MPU is not enabled (enableMPU in config)'
};


test('mpu auth', testOpts, function (suite) {
    var client;
    var subuserClient;
    var testAccount;
    var testDir;

    suite.test('setup: test account', function (t) {
        helper.ensureTestAccounts(t, function (err, accounts) {
            t.ifError(err, 'no error loading/creating test accounts');
            testAccount = accounts.regular;
            t.ok(testAccount, 'have regular test account: ' +
                testAccount.login);
            t.end();
        });
    });

    suite.test('setup: client and test dir', function (t) {
        subuserClient = helper.mantaClientFromSubuserInfo(testAccount,
            'muskietest_subuser');
        client = helper.mantaClientFromAccountInfo(testAccount);
        testDir = '/' + client.user + '/stor/test-mpu-auth-' +
            uuidv4().split('-')[0];

        client.mkdir(testDir, function (err) {
            t.ifError(err, 'make test testDir ' + testDir);
            t.end();
        });
    });


    // Subusers (not supported for MPU API)

    // Create
    suite.test('subusers disallowed: create', function (t) {
        var p = testDir + '/subusers-disallowed-create';
        subuserClient.createUpload(p, {
            account: client.user
        }, function (err, upload) {
            t.ok(err, 'expected error on subuser createUpload');
            t.ok(VError.hasCauseWithName(err,
                'AuthorizationFailedError'), err);
            t.end();
        });
    });

    // Get, upload, abort, commit.
    suite.test('subusers disallowed: get, upload, abort, commit, redirect',
    function (t) {
        var p = testDir + '/subusers-disallowed-get-upload-abort-commit';
        var context = {};

        vasync.pipeline({
            arg: context,
            funcs: [
                function realAccountCreatedUpload(ctx, next) {
                    client.createUpload(p, {
                        account: client.user
                    }, function (err, upload) {
                        ctx.upload = upload;
                        next(err);
                    });
                },

                function subuserGetIt(ctx, next) {
                    subuserClient.getUpload(ctx.upload.id, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ok(err, 'expect error on subuser getUpload');
                        t.ok(VError.hasCauseWithName(err,
                            'AuthorizationFailedError'),
                            'err is AuthorizationFailedError: ' + err);
                        next();
                    });
                },

                function subuserUploadPart(ctx, next) {
                    var data = 'The lazy brown fox \nsomething \nsomething foo';
                    var partNum = 0;
                    var stream = new MemoryStream();

                    subuserClient.uploadPart(stream, ctx.upload.id, partNum, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory,
                        // Generic client.put options.
                        md5: crypto
                            .createHash('md5')
                            .update(data)
                            .digest('base64'),
                        size: Buffer.byteLength(data),
                        type: 'text/plain'
                    }, function (err) {
                        t.ok(err, 'expect error on subuser uploadPart');
                        t.ok(VError.hasCauseWithName(err,
                            'NoMatchingRoleTagError'),
                            'err is NoMatchingRoleTagError: ' + err);
                        next();
                    });

                    setImmediate(function writeIt() {
                        stream.end(data);
                    });
                },

                function subuserAbortUpload(ctx, next) {
                    subuserClient.abortUpload(ctx.upload.id, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ok(err, 'expect error on subuser abortUpload');
                        t.ok(VError.hasCauseWithName(err,
                            'AuthorizationFailedError'),
                            'err is AuthorizationFailedError: ' + err);
                        next();
                    });
                },

                function subuserCommitUpload(ctx, next) {
                    subuserClient.commitUpload(ctx.upload.id, [], {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ok(err, 'expect error on subuser commitUpload');
                        t.ok(VError.hasCauseWithName(err,
                            'AuthorizationFailedError'),
                            'err is AuthorizationFailedError: ' + err);
                        next();
                    });
                },

                function subuserRedirect(ctx, next) {
                    var redirUrl = '/' + client.user + '/uploads/' +
                        ctx.upload.id;
                    subuserClient.get(redirUrl, function (err) {
                        t.ok(err,
                            'expect error on subuser GET /:login/uploads/:id');
                        t.ok(VError.hasCauseWithName(err,
                            'AuthorizationFailedError'),
                            'err is AuthorizationFailedError: ' + err);
                        next();
                    });
                },

                function subuserRedirectPartNum(ctx, next) {
                    var redirUrl = '/' + client.user + '/uploads/' +
                        ctx.upload.id + '/0';
                    subuserClient.get(redirUrl, function (err) {
                        t.ok(err, 'expect error on subuser ' +
                            'GET /:login/uploads/:id/:partNum');
                        t.ok(VError.hasCauseWithName(err,
                            'AuthorizationFailedError'),
                            'err is AuthorizationFailedError: ' + err);
                        next();
                    });
                }

            ]
        }, function (err) {
            t.ifError(err, 'expected no error in pipeline');

            if (context.upload) {
                // Abort this so it isn't lingering junk from the test suite.
                client.abortUpload(context.upload.id, {
                    account: client.user,
                    partsDirectory: context.upload.partsDirectory
                }, function (abortErr) {
                    t.ifError(abortErr,
                        'expected no error cleaning up test upload');
                    t.end();
                });
            } else {
                t.end();
            }
        });
    });


    suite.test('teardown', function (t) {
        client.rmr(testDir, function onRm(err) {
            t.ifError(err, 'remove test testDir ' + testDir);
            t.end();
        });
    });

    suite.end();
});
