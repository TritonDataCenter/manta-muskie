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

test('mpu del', testOpts, function (suite) {
    var testAccount;
    var testDir;
    var testOperAccount;
    var client;
    var operClient;

    suite.test('setup: test account', function (t) {
        helper.ensureTestAccounts(t, function (err, accounts) {
            t.ifError(err, 'no error loading/creating test accounts');
            testAccount = accounts.regular;
            t.ok(testAccount, 'have regular test account: ' +
                testAccount.login);
            testOperAccount = accounts.operator;
            t.ok(testOperAccount, 'have operator test account: ' +
                testOperAccount.login);

            client = helper.mantaClientFromAccountInfo(testAccount);
            operClient = helper.mantaClientFromAccountInfo(testOperAccount);
            testDir = '/' + client.user + '/stor/test-mpu-del-' +
                uuidv4().split('-')[0];

            t.end();
        });
    });

    suite.test('operator can delete an upload dir', function (t) {
        var p = testDir + '/del-this-upload-dir';

        vasync.pipeline({
            arg: {},
            funcs: [
                function createIt(ctx, next) {
                    client.createUpload(p, {
                        account: client.user
                    }, function (err, upload) {
                        t.ifError(err, 'expected success on createUpload');
                        ctx.upload = upload;
                        ctx.uploadPath = helper.mpuUploadPath(
                            client.user, upload.id);
                        t.comment('uploadPath: ' + ctx.uploadPath);
                        next(err);
                    });
                },

                function userCannotDeleteIt(ctx, next) {
                    client.unlink(ctx.uploadPath, {
                        query: {
                            allowMpuDeletes: true
                        }
                    }, function (err) {
                        t.ok(err, 'non-operator user cannot delete it');
                        t.ok(VError.hasCauseWithName(err,
                            'MethodNotAllowedError'),
                            'err is MethodNotAllowedError: ' + err);
                        next();
                    });
                },

                function mustProvideOverrideParam(ctx, next) {
                    operClient.unlink(ctx.uploadPath, {}, function (err, res) {
                        t.ok(err, 'operator cannot delete it without override');
                        t.ok(VError.hasCauseWithName(err,
                            'UnprocessableEntityError'),
                            'err is UnprocessableEntityError: ' + err);
                        next();
                    });
                },

                function overrideParamMustBeTrue(ctx, next) {
                    operClient.unlink(ctx.uploadPath, {
                        query: {
                            allowMpuDeletes: false
                        }
                    }, function (err, res) {
                        t.ok(err, 'operator cannot delete it with ' +
                            'allowMpuDeletes=false');
                        t.ok(VError.hasCauseWithName(err,
                            'UnprocessableEntityError'),
                            'err is UnprocessableEntityError: ' + err);
                        next();
                    });
                },

                function overrideParamMustBeBool(ctx, next) {
                    operClient.unlink(ctx.uploadPath, {
                        query: {
                            allowMpuDeletes: 1
                        }
                    }, function (err, res) {
                        t.ok(err,
                            'operator cannot delete it with allowMpuDeletes=1');
                        t.ok(VError.hasCauseWithName(err,
                            'UnprocessableEntityError'),
                            'err is UnprocessableEntityError: ' + err);
                        next();
                    });
                },

                function operatorCanDeleteIt(ctx, next) {
                    operClient.unlink(ctx.uploadPath, {
                        query: {
                            allowMpuDeletes: true
                        }
                    }, function (err, res) {
                        t.ifError(err,
                            'operator can delete it with allowMpuDeletes=true');
                        helper.assertMantaRes(t, res, 204);
                        next();
                    });
                }
            ]
        }, function (err) {
            t.ifError(err, 'expected no error in pipeline');
            t.end();
        });
    });

    suite.test('operator can delete an uploaded part', function (t) {
        var p = testDir + '/del-this-uploaded-part';
        var data = 'this is my part';

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

                    client.uploadPart(stream, ctx.upload.id, partNum, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err, res) {
                        t.ifError(err, 'expected success on uploadPart');

                        ctx.partPath = helper.mpuUploadPath(
                            client.user, ctx.upload.id, partNum);
                        t.comment('partPath: ' + ctx.partPath);

                        // TODO: Should really abort the upload if we fail here.
                        next(err);
                    });

                    setImmediate(function writeIt() {
                        stream.end(data);
                    });
                },

                function userCannotDeleteIt(ctx, next) {
                    client.unlink(ctx.partPath, {
                        query: {
                            allowMpuDeletes: true
                        }
                    }, function (err) {
                        t.ok(err, 'non-operator user cannot delete it');
                        t.ok(VError.hasCauseWithName(err,
                            'MethodNotAllowedError'),
                            'err is MethodNotAllowedError: ' + err);
                        next();
                    });
                },

                function mustProvideOverrideParam(ctx, next) {
                    operClient.unlink(ctx.partPath, {}, function (err, res) {
                        t.ok(err, 'operator cannot delete it without override');
                        t.ok(VError.hasCauseWithName(err,
                            'UnprocessableEntityError'),
                            'err is UnprocessableEntityError: ' + err);
                        next();
                    });
                },

                function overrideParamMustBeTrue(ctx, next) {
                    operClient.unlink(ctx.partPath, {
                        query: {
                            allowMpuDeletes: false
                        }
                    }, function (err, res) {
                        t.ok(err, 'operator cannot delete it with ' +
                            'allowMpuDeletes=false');
                        t.ok(VError.hasCauseWithName(err,
                            'UnprocessableEntityError'),
                            'err is UnprocessableEntityError: ' + err);
                        next();
                    });
                },

                function overrideParamMustBeBool(ctx, next) {
                    operClient.unlink(ctx.partPath, {
                        query: {
                            allowMpuDeletes: 1
                        }
                    }, function (err, res) {
                        t.ok(err,
                            'operator cannot delete it with allowMpuDeletes=1');
                        t.ok(VError.hasCauseWithName(err,
                            'UnprocessableEntityError'),
                            'err is UnprocessableEntityError: ' + err);
                        next();
                    });
                },

                function operatorCanDeleteIt(ctx, next) {
                    operClient.unlink(ctx.partPath, {
                        query: {
                            allowMpuDeletes: true
                        }
                    }, function (err, res) {
                        t.ifError(err,
                            'operator can delete it with allowMpuDeletes=true');
                        helper.assertMantaRes(t, res, 204);
                        next();
                    });
                }
            ]
        }, function (err) {
            t.ifError(err, 'expected no error in pipeline');
            t.end();
        });
    });

    suite.end();
});
