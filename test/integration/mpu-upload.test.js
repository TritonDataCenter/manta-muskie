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

var helper = require('../helper');
var mpuCommon = require('../../lib/uploads/common');


///--- Globals

var enableMPU = Boolean(require('../../etc/config.json').enableMPU);
var testOpts = {
    skip: !enableMPU && 'MPU is not enabled (enableMPU in config)'
};


///--- Tests

test('mpu upload', testOpts, function (suite) {
    var testAccount;
    var testDir;
    var client;

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
        client = helper.mantaClientFromAccountInfo(testAccount);
        testDir = '/' + client.user + '/stor/test-mpu-upload-' +
            uuidv4().split('-')[0];

        client.mkdir(testDir, function (err) {
            t.ifError(err, 'make test testDir ' + testDir);
            t.end();
        });
    });


    suite.test('upload part limits', function (t) {
        var p = testDir + '/upload-part-limits.txt';
        var data = 'This is my part data';

        vasync.pipeline({
            arg: {},
            funcs: [
                function createIt(ctx, next) {
                    client.createUpload(p, {
                        account: client.user
                    }, function (err, upload) {
                        t.ifError(err, 'expected success on createUpload');
                        ctx.upload = upload;
                        next(err);
                    });
                },

                function uploadMinPartNum(ctx, next) {
                    t.comment('min part num');

                    var partNum = mpuCommon.MIN_PART_NUM;
                    var stream = new MemoryStream();

                    client.uploadPart(stream, ctx.upload.id, partNum, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err, res) {
                        t.ifError(err, 'expected success on uploadPart');
                        helper.assertMantaRes(t, res, 204);
                        next();
                    });

                    setImmediate(function writeIt() {
                        stream.end(data);
                    });
                },

                function uploadMaxPartNum(ctx, next) {
                    t.comment('max part num');

                    var partNum = mpuCommon.MAX_PART_NUM;
                    var stream = new MemoryStream();

                    client.uploadPart(stream, ctx.upload.id, partNum, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err, res) {
                        t.ifError(err, 'expected success on uploadPart');
                        helper.assertMantaRes(t, res, 204);
                        next();
                    });

                    setImmediate(function writeIt() {
                        stream.end(data);
                    });
                },

                function uploadZeroBytePart(ctx, next) {
                    t.comment('zero byte part');

                    var partNum = 1;
                    var stream = new MemoryStream();

                    client.uploadPart(stream, ctx.upload.id, partNum, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory,
                        size: 0
                    }, function (err, res) {
                        t.ifError(err, 'expected success on uploadPart');
                        helper.assertMantaRes(t, res, 204);
                        next();
                    });

                    setImmediate(function writeIt() {
                        stream.end();
                    });
                },

                function uploadPartNumLessThanAllowed(ctx, next) {
                    t.comment('part number less than allowed');

                    var partNum = mpuCommon.MIN_PART_NUM - 1;
                    var stream = new MemoryStream();

                    client.uploadPart(stream, ctx.upload.id, partNum, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err, res) {
                        t.ok(err, 'expected error on uploadPart');
                        t.ok(VError.hasCauseWithName(err,
                            'MultipartUploadInvalidArgumentError'),
                            'err is MultipartUploadInvalidArgumentError');
                        helper.assertMantaRes(t, res, 409);
                        next();
                    });

                    setImmediate(function writeIt() {
                        stream.end(data);
                    });
                },

                function uploadPartNumMoreThanAllowed(ctx, next) {
                    t.comment('part number more than allowed');

                    var partNum = mpuCommon.MAX_PART_NUM + 1;
                    var stream = new MemoryStream();

                    client.uploadPart(stream, ctx.upload.id, partNum, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err, res) {
                        t.ok(err, 'expected error on uploadPart');
                        t.ok(VError.hasCauseWithName(err,
                            'MultipartUploadInvalidArgumentError'),
                            'err is MultipartUploadInvalidArgumentError');
                        helper.assertMantaRes(t, res, 409);
                        next();
                    });

                    setImmediate(function writeIt() {
                        stream.end(data);
                    });
                },

                function uploadDurabilityLevelHeader(ctx, next) {
                    t.comment('set durability-level header for uploadPart');

                    var partNum = 1;
                    var stream = new MemoryStream();

                    client.uploadPart(stream, ctx.upload.id, partNum, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory,
                        headers: {
                            'durability-level': 2
                        }
                    }, function (err, res) {
                        t.ok(err, 'expected error on uploadPart');
                        t.ok(VError.hasCauseWithName(err,
                            'MultipartUploadInvalidArgumentError'),
                            'err is MultipartUploadInvalidArgumentError');
                        helper.assertMantaRes(t, res, 409);
                        next();
                    });

                    setImmediate(function writeIt() {
                        stream.end(data);
                    });
                },

                function uploadXDurabilityLevelHeader(ctx, next) {
                    t.comment('set x-durability-level header for uploadPart');

                    var partNum = 1;
                    var stream = new MemoryStream();

                    client.uploadPart(stream, ctx.upload.id, partNum, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory,
                        headers: {
                            'x-durability-level': 2
                        }
                    }, function (err, res) {
                        t.ok(err, 'expected error on uploadPart');
                        t.ok(VError.hasCauseWithName(err,
                            'MultipartUploadInvalidArgumentError'),
                            'err is MultipartUploadInvalidArgumentError');
                        helper.assertMantaRes(t, res, 409);
                        next();
                    });

                    setImmediate(function writeIt() {
                        stream.end(data);
                    });
                },

                function uploadUsingNonExistantId(ctx, next) {
                    t.comment('upload using non-existant upload ID');

                    var partNum = 1;
                    var stream = new MemoryStream();

                    client.uploadPart(stream, ctx.upload.id, partNum, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory,
                        headers: {
                            'x-durability-level': 2
                        }
                    }, function (err, res) {
                        t.ok(err, 'expected error on uploadPart');
                        t.ok(VError.hasCauseWithName(err,
                            'MultipartUploadInvalidArgumentError'),
                            'err is MultipartUploadInvalidArgumentError');
                        helper.assertMantaRes(t, res, 409);
                        next();
                    });

                    setImmediate(function writeIt() {
                        stream.end(data);
                    });
                },

                function cleanupAbortUpload(ctx, next) {
                    t.comment('cleanup: abort the test upload');
                    client.abortUpload(ctx.upload.id, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ifError(err, 'expected success from abortUpload');
                        next();
                    });
                }
            ]
        }, function (err) {
            t.ifError(err, 'expected success from pipeline');
            t.end();
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
