/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var MemoryStream = require('stream').PassThrough;
var test = require('tap').test;
var uuidv4 = require('uuid/v4');
var vasync = require('vasync');
var VError = require('verror');

var helper = require('../helper');


///--- Globals

var enableMPU = Boolean(require('../../etc/config.json').enableMPU);
var testOpts = {
    skip: !enableMPU && 'MPU is not enabled (enableMPU in config)'
};


///--- Tests

test('mpu redirect', testOpts, function (suite) {
    var testAccount;
    var testDir;
    var client;

    suite.test('setup: test account', function (t) {
        helper.ensureTestAccounts(t, function (err, accounts) {
            t.ifError(err, 'no error loading/creating test accounts');
            testAccount = accounts.regular;
            t.ok(testAccount, 'have regular test account: ' + testAccount.login);
            t.end();
        });
    });

    suite.test('setup: client and test dir', function (t) {
        client = helper.mantaClientFromAccountInfo(testAccount);
        testDir = '/' + client.user + '/stor/test-mpu-redirect-' +
            uuidv4().split('-')[0];

        client.mkdir(testDir, function (err) {
            t.ifError(err, 'make test testDir ' + testDir);
            t.end();
        });
    });

    suite.test('redirect upload dir /:account/uploads/:id', function (t) {
        var p = testDir + '/redir-upload-dir.txt';

        vasync.pipeline({
            arg: {},
            funcs: [
                function createIt(ctx, next) {
                    client.createUpload(p, {
                        account: client.user
                    }, function (err, upload) {
                        t.ifError(err, 'expected success on createUpload');

                        ctx.upload = upload;
                        ctx.redirPath = `/${client.user}/uploads/${upload.id}`;
                        ctx.uploadPath = helper.mpuUploadPath(
                            client.user, upload.id);

                        next(err);
                    });
                },

                function checkGET(ctx, next) {
                    t.comment('GET');
                    client.get(ctx.redirPath, function (err, _stream, res) {
                        t.ifError(err, 'expected success on GET');
                        helper.assertMantaRes(t, res, 301);
                        t.equal(res.headers.location, ctx.uploadPath,
                            '"location" header');
                        next();
                    });
                },

                function checkHEAD(ctx, next) {
                    t.comment('HEAD');
                    client.info(ctx.redirPath, function (err, info, res) {
                        t.ifError(err, 'expected success on HEAD');
                        if (!err) {
                            helper.assertMantaRes(t, res, 301);
                            t.equal(res.headers.location, ctx.uploadPath,
                                '"location" header');
                        }
                        next();
                    });
                },

                function checkPUT(ctx, next) {
                    t.comment('PUT');
                    var stream = new MemoryStream();
                    client.put(ctx.redirPath, stream, function (err, res) {
                        t.ifError(err, 'expected success on PUT');
                        helper.assertMantaRes(t, res, 301);
                        t.equal(res.headers.location, ctx.uploadPath,
                            '"location" header');
                        next();
                    });
                },

                function cleanupAbortUpload(ctx, next) {
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

    suite.test('redirect part /:account/uploads/:id/:partNum', function (t) {
        var p = testDir + '/redir-upload-part.txt';

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

                function uploadOnePart(ctx, next) {
                    var partNum = 0;
                    var stream = new MemoryStream();
                    var data = 'This is my part';

                    client.uploadPart(stream, ctx.upload.id, partNum, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err, res) {
                        t.ifError(err, 'expected success on uploadPart');

                        ctx.partEtags = [res.headers.etag];
                        ctx.redirPath = `/${client.user}/uploads/${ctx.upload.id}/${partNum}`;
                        ctx.uploadPath = helper.mpuUploadPath(
                            client.user, ctx.upload.id, partNum);

                        next();
                    });

                    setImmediate(function writeIt() {
                        stream.end(data);
                    });
                },

                function checkGET(ctx, next) {
                    t.comment('GET');
                    client.get(ctx.redirPath, function (err, _stream, res) {
                        t.ifError(err, 'expected success on GET');
                        helper.assertMantaRes(t, res, 301);
                        t.equal(res.headers.location, ctx.uploadPath,
                            '"location" header');
                        next();
                    });
                },

                function checkHEAD(ctx, next) {
                    t.comment('HEAD');
                    client.info(ctx.redirPath, function (err, info, res) {
                        t.ifError(err, 'expected success on HEAD');
                        if (!err) {
                            helper.assertMantaRes(t, res, 301);
                            t.equal(res.headers.location, ctx.uploadPath,
                                '"location" header');
                        }
                        next();
                    });
                },

                function checkPUT(ctx, next) {
                    t.comment('PUT');
                    var stream = new MemoryStream();
                    client.put(ctx.redirPath, stream, function (err, res) {
                        t.ifError(err, 'expected success on PUT');
                        helper.assertMantaRes(t, res, 301);
                        t.equal(res.headers.location, ctx.uploadPath,
                            '"location" header');
                        next();
                    });
                },

                function cleanupAbortUpload(ctx, next) {
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
