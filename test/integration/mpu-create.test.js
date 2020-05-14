/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var crypto = require('crypto');

var test = require('tap').test;
var uuidv4 = require('uuid/v4');
var vasync = require('vasync');
var VError = require('verror');

var helper = require('../helper');
var obj = require('../../lib/obj');


///--- Globals

var enableMPU = Boolean(require('../../etc/config.json').enableMPU);
var testOpts = {
    skip: !enableMPU && 'MPU is not enabled (enableMPU in config)'
};


///--- Tests

test('mpu create', testOpts, function (suite) {
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
        testDir = '/' + client.user + '/stor/test-mpu-create-' +
            uuidv4().split('-')[0];

        client.mkdir(testDir, function (err) {
            t.ifError(err, 'make test testDir ' + testDir);
            t.end();
        });
    });


    suite.test('create params and headers', function (t) {
        var p = testDir + '/create-params-and-headers';
        var size = 42;
        var copies = 2;
        var data = 'this is my part';
        var md5 = crypto.createHash('md5').update(data).digest('base64');
        var headers = {
            'm-my-custom-header': 'my-custom-value',
            'm-MiXeD-CaSe': 'MiXeD-CaSe',
            'content-disposition': 'attachment; filename="my-file.txt"'
        };

        vasync.pipeline({
            arg: {},
            funcs: [
                function createIt(ctx, next) {
                    client.createUpload(p, {
                        account: client.user,
                        // Optional params:
                        size: size,
                        copies: copies,
                        md5: md5,
                        headers: headers
                    }, function (err, upload) {
                        t.ifError(err, 'expected success on createUpload');
                        ctx.upload = upload;
                        next(err);
                    });
                },

                function getItAndCheckThings(ctx, next) {
                    client.getUpload(ctx.upload.id, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err, upload) {
                        t.ifError(err, 'expected success on getUpload');

                        t.equal(upload.state, 'created', 'upload.state');
                        t.ok(upload.creationTimeMs, 'upload.creationTimeMs');
                        t.equal(upload.headers['content-length'], size,
                            'upload.headers["content-length"]');
                        t.equal(upload.headers['durability-level'], copies,
                            'upload.headers["durability-level"]');
                        t.equal(upload.headers['content-md5'], md5,
                            'upload.headers["content-md5"]');
                        t.equal(upload.headers['m-my-custom-header'],
                            headers['m-my-custom-header'],
                            'upload.headers["m-my-custom-header"]');
                        // Header names are case-insensitive (and are
                        // lowercased).
                        t.equal(upload.headers['m-mixed-case'],
                            headers['m-MiXeD-CaSe'],
                            'upload.headers["m-mixed-case"]');
                        t.equal(upload.headers['content-disposition'],
                            headers['content-disposition'],
                            'upload.headers["content-disposition"]');

                        next();
                    });
                },

                function cleanupAbortIt(ctx, next) {
                    client.abortUpload(ctx.upload.id, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ifError(err, 'expected success from abortUpload');
                        next(err);
                    });
                }
            ]
        }, function (err) {
            t.ifError(err, 'expected no error in pipeline');
            t.end();
        });
    });

    suite.test('create failures on bad args', function (t) {
        var cases = [
            {
                // object path under a nonexistent account
                path: '/muskietest_no_such_account/stor/foo.txt',
                headers: null,
                errName: 'AccountDoesNotExistError'
            },
            {
                // if-match header disallowed
                path: testDir + '/foo.txt',
                headers: {
                    'if-match': 'foo'
                },
                errName: 'MultipartUploadInvalidArgumentError'
            },
            {
                path: testDir + '/foo.txt',
                headers: {
                    'if-none-match': 'foo'
                },
                errName: 'MultipartUploadInvalidArgumentError'
            },
            {
                path: testDir + '/foo.txt',
                headers: {
                    'if-modified-since': 'foo'
                },
                errName: 'MultipartUploadInvalidArgumentError'
            },
            {
                path: testDir + '/foo.txt',
                headers: {
                    'if-unmodified-since': 'foo'
                },
                errName: 'MultipartUploadInvalidArgumentError'
            },
            {
                // Silly negative content-length.
                path: testDir + '/foo.txt',
                headers: {
                    'content-length': -1
                },
                errName: 'MultipartUploadInvalidArgumentError'
            },
            {
                // Too many copies.
                path: testDir + '/foo.txt',
                headers: {
                    'durability-level': obj.DEF_MAX_COPIES + 1
                },
                errName: 'InvalidDurabilityLevelError'
            },
            {
                // Too few copies.
                path: testDir + '/foo.txt',
                headers: {
                    'durability-level': obj.DEF_MIN_COPIES - 1
                },
                errName: 'InvalidDurabilityLevelError'
            },
            {
                // Bogus content-disposition.
                path: testDir + '/foo.txt',
                headers: {
                    'content-disposition': 'attachment;'
                },
                errName: 'BadRequestError'
            }
        ];

        vasync.forEachPipeline({
            inputs: cases,
            func: function oneCase(c, nextCase) {
                t.comment('test case: ' + JSON.stringify(c));

                client.createUpload(c.path, {
                    account: client.user,
                    headers: c.headers
                }, function (err, upload) {
                    t.ok(err, 'expected err from createUpload');
                    t.ok(VError.hasCauseWithName(err, c.errName),
                        'err is ' + c.errName + ': ' + err);
                    nextCase();
                });
            }
        }, function (err) {
            t.ifError(err, 'expect no error from cases');
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
