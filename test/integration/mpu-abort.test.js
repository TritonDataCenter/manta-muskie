/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var test = require('tap').test;
var path = require('path');
var uuidv4 = require('uuid/v4');
var vasync = require('vasync');
var VError = require('verror');

// XXX
//var testHelper = require('../helper.js');
var helper = require('../helper.js');

var ifErr = helper.ifErr;

var enableMPU = Boolean(require('../../etc/config.json').enableMPU);
var testOpts = {
    skip: !enableMPU && 'MPU is not enabled (enableMPU in config)'
};

// XXX
//before(function (cb) {
//    helper.initMPUTester.call(this, cb);
//});
//
//
//after(function (cb) {
//    helper.cleanupMPUTester.call(this, cb);
//});

test('mpu abort', testOpts, function (suite) {
    var client;
    var jsonClient;
    var testAccount;
    var testDir;

    suite.test('setup: test account', function (t) {
        helper.ensureTestAccounts(t, function (err, accounts) {
            t.ifError(err, 'no error loading/creating test accounts');
            testAccount = accounts.regular;
            t.ok(testAccount, 'have regular test account: ' + testAccount.login);
            testOperAccount = accounts.operator;
            t.ok(testOperAccount,
                'have operator test account: ' + testOperAccount.login);
            t.end();
        });
    });

    suite.test('setup: client and test dir', function (t) {
        jsonClient = helper.createJsonClient();
        client = helper.mantaClientFromAccountInfo(testAccount);
        testDir = '/' + client.user + '/stor/test-mpu-abort-' +
            uuidv4().split('-')[0];

        client.mkdir(testDir, function (err) {
            t.ifError(err, 'make test testDir ' + testDir);
            t.end();
        });
    });

    suite.test('abort upload', function (t) {
        var p = testDir + '/abort-upload';
        vasync.pipeline({
            arg: {},
            funcs: [
                function createIt(ctx, next) {
                    client.createUpload(p, {
                        account: client.user
                    }, function (err, upload) {
                        t.ok(upload.id, 'created upload, id=' + upload.id);
                        t.ok(upload.partsDirectory);
                        ctx.upload = upload;
                        next(err);
                    });
                },
                function abortIt(ctx, next) {
                    client.abortUpload(ctx.upload.id, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, next);
                },
                function getIt(ctx, next) {
                    client.getUpload(ctx.upload.id, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err, upload) {
                        t.deepEqual(upload.headers, {},
                            'upload headers are empty');
                        t.equal(upload.state, 'done', 'upload.state');
                        t.equal(upload.result, 'aborted', 'upload.result');
                        next(err);
                    });
                }
            ]
        }, function (err) {
            t.ifError(err, 'expected no error running pipeline');
            t.end();
        });
    });


    suite.test('abort upload, upload already aborted', function (t) {
        var p = testDir + '/abort-upload-already-aborted';
        vasync.pipeline({
            arg: {},
            funcs: [
                function createIt(ctx, next) {
                    client.createUpload(p, {
                        account: client.user
                    }, function (err, upload) {
                        t.ok(upload.id, 'created upload, id=' + upload.id);
                        t.ok(upload.partsDirectory);
                        ctx.upload = upload;
                        next(err);
                    });
                },
                function abortIt(ctx, next) {
                    client.abortUpload(ctx.upload.id, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, next);
                },
                function abortItAgain(ctx, next) {
                    client.abortUpload(ctx.upload.id, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, next);
                },
                function getIt(ctx, next) {
                    client.getUpload(ctx.upload.id, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err, upload) {
                        t.deepEqual(upload.headers, {},
                            'upload headers are empty');
                        t.equal(upload.state, 'done', 'upload.state');
                        t.equal(upload.result, 'aborted', 'upload.result');
                        next(err);
                    });
                }
            ]
        }, function (err) {
            t.ifError(err, 'expected no error running pipeline');
            t.end();
        });
    });


    // Abort: bad input

    suite.test('abort upload: upload already committed', function (t) {
        var p = testDir + '/abort-upload-already-committed';
        vasync.pipeline({
            arg: {},
            funcs: [
                function createIt(ctx, next) {
                    client.createUpload(p, {
                        account: client.user
                    }, function (err, upload) {
                        t.ok(upload.id, 'created upload, id=' + upload.id);
                        t.ok(upload.partsDirectory);
                        ctx.upload = upload;
                        next(err);
                    });
                },
                function commitIt(ctx, next) {
                    client.commitUpload(ctx.upload.id, [], {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.comment('commited upload ' + ctx.upload.id);
                        next(err);
                    });
                },
                function abortIt(ctx, next) {
                    client.abortUpload(ctx.upload.id, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ok(err, 'expected error aborting');
                        t.ok(VError.hasCauseWithName(err,
                            'InvalidMultipartUploadStateError'),
                            'expected err chain includes ' +
                                'InvalidMultipartUploadStateError');
                        next();
                    });
                }
            ]
        }, function (err) {
            t.ifError(err, 'expected no error running pipeline');
            t.end();
        });
    });


    suite.test('abort upload: non-uuid id', function (t) {
        var action = 'abort';
        var bogus = 'foobar';
        var postOpts = {
            headers: {
                'content-type': 'application/json',
                'accept': 'application/json'
            },
            path: '/' + client.user + '/uploads/f/' + bogus + '/' + action
        };

        client.signRequest({
            headers: postOpts.headers
        }, function (signErr) {
            if (helper.ifErr(t, signErr, 'sign request')) {
                t.end();
                return;
            }

            // We use the jsonClient directly, or we will blow a non-uuid assert
            // in the Manta client.
            jsonClient.post(postOpts, {}, function (err, _, res) {
                t.ok(err);
                t.ok(VError.hasCauseWithName(err, 'ResourceNotFoundError'));
                helper.assertMantaRes(t, res, 404);
                t.end();
            });
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
