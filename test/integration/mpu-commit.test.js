/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var assert = require('assert');
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


/// ---- helper functions

// Given an array of etags, returns the md5 we expect from the MPU API.
function computePartsMD5(parts) {
    var hash = crypto.createHash('md5');
    parts.forEach(function (p) {
        hash.update(p);
    });

    return (hash.digest('base64'));
}


///--- Tests

test('mpu commit', testOpts, function (suite) {
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
        testDir = '/' + client.user + '/stor/test-mpu-commit-' +
            uuidv4().split('-')[0];

        client.mkdir(testDir, function (err) {
            t.ifError(err, 'make test testDir ' + testDir);
            t.end();
        });
    });

    suite.test('commit upload: zero parts', function (t) {
        var p = testDir + '/commit-upload-zero-parts';

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

                function commitIt(ctx, next) {
                    client.commitUpload(ctx.upload.id, [], {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ifError(err, 'expected success on commitUpload');
                        next(err);
                    });
                },

                function getIt(ctx, next) {
                    client.getUpload(ctx.upload.id, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err, upload) {
                        t.ifError(err, 'expected success on getUpload');
                        t.deepEqual(upload.headers, {});
                        t.equal(upload.state, 'done', 'upload.state');
                        t.equal(upload.result, 'committed', 'upload.result');
                        t.equal(upload.partsMD5Summary, computePartsMD5([]),
                            'upload.partsMD5Summary');
                        next();
                    });
                }
            ]
        }, function (err) {
            t.ifError(err, 'expected no error in pipeline');
            t.end();
        });
    });

    suite.test('commit upload: one part', function (t) {
        var p = testDir + '/commit-upload-one-part';

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
                    var data = 'The lazy brown fox \nsomething \nsomething foo';
                    var partNum = 0;
                    var stream = new MemoryStream();

                    ctx.dataMd5 = crypto.createHash('md5').update(data)
                        .digest('base64');

                    client.uploadPart(stream, ctx.upload.id, partNum, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory,
                        // Generic client.put options.
                        md5: ctx.dataMd5,
                        size: Buffer.byteLength(data),
                        type: 'text/plain'
                    }, function (err, res) {
                        t.ifError(err, 'expected success on uploadPart');
                        helper.assertMantaRes(t, res, 204);
                        if (!err) {
                            t.ok(res.headers.etag, 'res.headers.etag');
                            ctx.partEtags = [res.headers.etag];
                        }
                        // TODO: Should really abort the upload if we fail here.
                        next(err);
                    });

                    setImmediate(function writeIt() {
                        stream.end(data);
                    });
                },


                // Commit it with duplicate parts etags. This should fail.
                function commitItWithDupeParts(ctx, next) {
                    var parts = [ctx.partEtags[0], ctx.partEtags[0]];
                    client.commitUpload(ctx.upload.id, parts, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ok(err, 'expected fail on commitUpload with ' +
                            'dupe parts etags');
                        t.ok(VError.hasCauseWithName(err,
                            'MultipartUploadInvalidArgumentError'),
                            'err is MultipartUploadInvalidArgumentError');
                        next();
                    });
                },

                function commitIt(ctx, next) {
                    client.commitUpload(ctx.upload.id, ctx.partEtags, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err, res) {
                        t.ifError(err, 'expected success on commitUpload');
                        t.equal(res.headers['computed-md5'], ctx.dataMd5,
                            '"computed-md5" header matches data MD5');
                        next(err);
                    });
                },

                // HEAD the commited file and check its content-md5 matches.
                function checkContentMd5(ctx, next) {
                    client.info(p, function (err, info) {
                        t.ifError(err, 'expected client.info success');
                        t.equal(info.headers['content-md5'], ctx.dataMd5,
                            '"content-md5" header matches data MD5');
                        next(err);
                    });
                },

                // Test that a second commit to an already commited upload
                // *works* (using the same array of parts etags, that is).
                function commitItAgainWithSameParts(ctx, next) {
                    client.commitUpload(ctx.upload.id, ctx.partEtags, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ifError(err, 'expected success on re-commitUpload');
                        next(err);
                    });
                },

                function getIt(ctx, next) {
                    client.getUpload(ctx.upload.id, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err, upload) {
                        t.ifError(err, 'expected success on getUpload');
                        t.deepEqual(upload.headers, {});
                        t.equal(upload.state, 'done', 'upload.state');
                        t.equal(upload.result, 'committed', 'upload.result');
                        t.equal(upload.partsMD5Summary,
                            computePartsMD5(ctx.partEtags),
                            'upload.partsMD5Summary');
                        next();
                    });
                }
            ]
        }, function (err) {
            t.ifError(err, 'expected no error in pipeline');
            t.end();
        });
    });

    suite.test('cannot commit after already aborted', function (t) {
        var p = testDir + '/commit-upload-already-aborted';
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
                    }, function (err) {
                        t.ifError(err, 'expected success from abortUpload');
                        next(err);
                    });
                },
                function commitIt(ctx, next) {
                    client.commitUpload(ctx.upload.id, [], {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ok(err, 'expected fail in commitUpload after abort');
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

    suite.test('commit content-length does not match create header',
    function (t) {
        var wrongContentLen = 6;
        var data = 'The lazy brown fox \nsomething \nsomething foo';
        assert(Buffer.byteLength(data) !== wrongContentLen);
        var p = testDir + '/commit-and-create-size-do-not-match';

        vasync.pipeline({
            arg: {},
            funcs: [
                function createIt(ctx, next) {
                    client.createUpload(p, {
                        account: client.user,
                        size: wrongContentLen
                    }, function (err, upload) {
                        t.ok(upload.id, 'created upload, id=' + upload.id);
                        ctx.upload = upload;
                        next(err);
                    });
                },

                function commitItWithLenMismatch0Parts(ctx, next) {
                    client.commitUpload(ctx.upload.id, [], {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ok(err, 'expected fail in commitUpload (0 parts)');
                        t.ok(VError.hasCauseWithName(err,
                            'MultipartUploadInvalidArgumentError'),
                            'err is MultipartUploadInvalidArgumentError: ' +
                                err);
                        next();
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
                        ctx.partEtags = [res.headers.etag];
                        next();
                    });

                    setImmediate(function writeIt() {
                        stream.end(data);
                    });
                },

                function commitItWithLenMismatch1Part(ctx, next) {
                    client.commitUpload(ctx.upload.id, ctx.partEtags, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ok(err, 'expected fail in commitUpload (1 part)');
                        t.ok(VError.hasCauseWithName(err,
                            'MultipartUploadInvalidArgumentError'),
                            'err is MultipartUploadInvalidArgumentError: ' +
                                err);
                        next();
                    });
                },

                function cleanUpAbortIt(ctx, next) {
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
            t.ifError(err, 'expected no error running pipeline');
            t.end();
        });
    });

    suite.test('commit content-md5 does not match create header: 0 parts',
    function (t) {
        var wrongContentMd5 = crypto.createHash('md5')
            .update('not the data').digest('base64');
        var p = testDir + '/commit-and-create-content-md5-do-not-match-0-parts';

        vasync.pipeline({
            arg: {},
            funcs: [
                function createIt(ctx, next) {
                    client.createUpload(p, {
                        account: client.user,
                        md5: wrongContentMd5
                    }, function (err, upload) {
                        t.ok(upload.id, 'created upload, id=' + upload.id);
                        ctx.upload = upload;
                        next(err);
                    });
                },

                function commitFail0Parts(ctx, next) {
                    client.commitUpload(ctx.upload.id, [], {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ok(err, 'expected fail in commitUpload (0 parts)');
                        t.ok(VError.hasCauseWithName(err,
                            'MultipartUploadInvalidArgumentError'),
                            'err is MultipartUploadInvalidArgumentError: ' +
                                err);
                        next();
                    });
                }

                // Apparently after this commit failure *due to content-md5
                // mismatch*, the state of the upload is `finalizing`.
                // An immediate subsequent "abortUpload" fails with `already
                // committed`. This differs from the commit failure *due to
                // content-length mismatch* in the previous test case. I'm
                // not sure if this is a minor MPU bug.
            ]
        }, function (err) {
            t.ifError(err, 'expected no error running pipeline');
            t.end();
        });
    });

    suite.test('commit content-md5 does not match create header: 1 part',
    function (t) {
        var wrongContentMd5 = crypto.createHash('md5')
            .update('not the data').digest('base64');
        var data = 'The lazy brown fox \nsomething \nsomething foo';
        var p = testDir + '/commit-and-create-content-md5-do-not-match-1-part';

        vasync.pipeline({
            arg: {},
            funcs: [
                function createIt(ctx, next) {
                    client.createUpload(p, {
                        account: client.user,
                        md5: wrongContentMd5
                    }, function (err, upload) {
                        t.ok(upload.id, 'created upload, id=' + upload.id);
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
                        ctx.partEtags = [res.headers.etag];
                        next();
                    });

                    setImmediate(function writeIt() {
                        stream.end(data);
                    });
                },

                function commitFail1Part(ctx, next) {
                    client.commitUpload(ctx.upload.id, ctx.partEtags, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ok(err, 'expected fail in commitUpload (1 part)');
                        t.ok(VError.hasCauseWithName(err,
                            'MultipartUploadInvalidArgumentError'),
                            'err is MultipartUploadInvalidArgumentError: ' +
                                err);
                        next();
                    });
                }
            ]
        }, function (err) {
            t.ifError(err, 'expected no error running pipeline');
            t.end();
        });
    });


    suite.test('commit upload: non-final part less than min part size',
    function (t) {
        var data = 'This is my part';
        var p = testDir + '/non-final-part-less-than-min-part-size';

        vasync.pipeline({
            arg: {},
            funcs: [
                function createIt(ctx, next) {
                    client.createUpload(p, {
                        account: client.user
                    }, function (err, upload) {
                        t.ok(upload.id, 'created upload, id=' + upload.id);
                        ctx.upload = upload;
                        next(err);
                    });
                },

                function uploadSomeSmallParts(ctx, next) {
                    ctx.partEtags = [];

                    vasync.forEachParallel({
                        inputs: [0, 1, 2],
                        func: function uploadOnePart(partNum, nextPart) {
                            var stream = new MemoryStream();

                            client.uploadPart(stream, ctx.upload.id, partNum, {
                                account: client.user,
                                partsDirectory: ctx.upload.partsDirectory
                            }, function (err, res) {
                                t.ifError(err,
                                    'expected success on uploadPart ' +
                                        partNum);
                                ctx.partEtags.push(res.headers.etag);
                                nextPart(err);
                            });

                            setImmediate(function writeIt() {
                                stream.end(data);
                            });
                        }
                    }, function (err) {
                        next(err);
                    });
                },

                function commitFail1Part(ctx, next) {
                    client.commitUpload(ctx.upload.id, ctx.partEtags, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ok(err, 'expected fail in commitUpload');
                        t.ok(VError.hasCauseWithName(err,
                            'MultipartUploadInvalidArgumentError'),
                            'err is MultipartUploadInvalidArgumentError: ' +
                                err);
                        next();
                    });
                }
            ]
        }, function (err) {
            t.ifError(err, 'expected no error running pipeline');
            t.end();
        });
    });


    suite.test('commit failures on bad paths', function (t) {
        var cases = [
            {
                // Top-level dir.
                path: '/' + client.user + '/stor',
                errName: 'OperationNotAllowedOnDirectoryError'
            },
            {
                // Parent dir does not exist.
                path: testDir + '/no-such-dir/foo.txt',
                errName: 'DirectoryDoesNotExistError'
            },
            {
                // Path under another account.
                path: '/poseidon/stor/muskietest_mpu_commit.txt',
                errName: 'AuthorizationFailedError'
            }
        ];

        vasync.forEachPipeline({
            inputs: cases,
            func: function oneCase(c, nextCase) {
                t.comment('test case: ' + JSON.stringify(c));

                // Create an upload with a bad path (this succeeds).
                client.createUpload(c.path, {
                    account: client.user
                }, function (createErr, upload) {
                    t.ifError(createErr, 'expected success from createUpload');

                    // Commit it. This should fail.
                    client.commitUpload(upload.id, [], {
                        account: client.user,
                        partsDirectory: upload.partsDirectory
                    }, function (commitErr) {
                        t.ok(commitErr, 'expected err on commit to ' + c.path);
                        t.ok(VError.hasCauseWithName(commitErr, c.errName),
                            'err is ' + c.errName + ': ' + commitErr);

                        // Clean up by aborting.
                        client.abortUpload(upload.id, {
                            account: client.user,
                            partsDirectory: upload.partsDirectory
                        }, function (err) {
                            t.ifError(err, 'expected success from abortUpload');
                            nextCase();
                        });
                    });
                });
            }
        }, function (err) {
            t.ifError(err, 'expect no error from cases');
            t.end();
        });
    });

    suite.test('commit failures from bad etags array', function (t) {
        var cases = [
            {
                // Empty string etag.
                partEtags: ['']
            },
            {
                // Multiple empty string entries.
                // "ETAG" here is replaced with our actual part 0 etag.
                partEtags: ['', 'ETAG', '']
            },
            {
                // Bogus etag value.
                partEtags: ['bogus']
            },
            {
                // Undefined (certainly a possibility in JS code using the
                // client).
                partEtags: undefined
            }
        ];
        var data = 'this is my part';
        var p = testDir + '/commit-fail-from-bad-etags';

        vasync.forEachPipeline({
            inputs: cases,
            func: function oneCase(c, nextCase) {
                t.comment('test case: ' + JSON.stringify(c));

                vasync.pipeline({
                    arg: {},
                    funcs: [
                        function createIt(ctx, next) {
                            client.createUpload(p, {
                                account: client.user
                            }, function (err, upload) {
                                t.ifError(err,
                                    'expected success on createUpload');
                                ctx.upload = upload;
                                next(err);
                            });
                        },

                        function uploadOnePart(ctx, next) {
                            var stream = new MemoryStream();

                            client.uploadPart(stream, ctx.upload.id, 0, {
                                account: client.user,
                                partsDirectory: ctx.upload.partsDirectory
                            }, function (err, res) {
                                t.ifError(err,
                                    'expected success on uploadPart');
                                ctx.part0Etag = res.headers.etag;
                                next(err);
                            });

                            setImmediate(function writeIt() {
                                stream.end(data);
                            });
                        },

                        function commitFail(ctx, next) {
                            var partEtags = c.partEtags;
                            if (partEtags) {
                                partEtags = partEtags
                                    .map(function (e) {
                                        // JSSTYLED
                                        return e.replace('ETAG', ctx.part0Etag);
                                    });
                            }

                            client.commitUpload(ctx.upload.id, partEtags, {
                                account: client.user,
                                partsDirectory: ctx.upload.partsDirectory
                            }, function (err) {
                                t.ok(err, 'expected fail in commitUpload ' +
                                    'with bogus partEtags: ' +
                                    JSON.stringify(partEtags));
                                t.ok(VError.hasCauseWithName(err,
                                    'MultipartUploadInvalidArgumentError'),
                                    // JSSTYLED
                                    'err is MultipartUploadInvalidArgumentError: ' + err);
                                next();
                            });
                        },

                        function cleanupAbortUpload(ctx, next) {
                            client.abortUpload(ctx.upload.id, {
                                account: client.user,
                                partsDirectory: ctx.upload.partsDirectory
                            }, function (err) {
                                t.ifError(err,
                                    'expected success from abortUpload');
                                next();
                            });
                        }
                    ]
                }, function (err) {
                    t.ifError(err, 'expected no error from pipeline');
                    nextCase();
                });
            }
        }, function (err) {
            t.ifError(err, 'expect no error from cases');
            t.end();
        });
    });


    suite.test('commit upload: more than max parts specified', function (t) {
        var data = 'This is my part';
        var p = testDir + '/too-many-parts';

        vasync.pipeline({
            arg: {},
            funcs: [
                function createIt(ctx, next) {
                    client.createUpload(p, {
                        account: client.user
                    }, function (err, upload) {
                        t.ok(upload.id, 'created upload, id=' + upload.id);
                        ctx.upload = upload;
                        next(err);
                    });
                },

                function uploadAPart(ctx, next) {
                    var stream = new MemoryStream();

                    client.uploadPart(stream, ctx.upload.id, 0, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err, res) {
                        t.ifError(err, 'expected success on uploadPart');
                        ctx.part0Etag = res.headers.etag;
                        next(err);
                    });

                    setImmediate(function writeIt() {
                        stream.end(data);
                    });
                },

                function commitWithTooManyParts(ctx, next) {
                    var partEtags = [];
                    for (var i = 0; i <= mpuCommon.MAX_NUM_PARTS + 1; i++) {
                        partEtags.push(ctx.part0Etag);
                    }

                    client.commitUpload(ctx.upload.id, partEtags, {
                        account: client.user,
                        partsDirectory: ctx.upload.partsDirectory
                    }, function (err) {
                        t.ok(err, 'expected fail on commitUpload');
                        t.ok(VError.hasCauseWithName(err,
                            'MultipartUploadInvalidArgumentError'),
                            'err is MultipartUploadInvalidArgumentError: ' +
                                err);
                        next();
                    });
                }
            ]
        }, function (err) {
            t.ifError(err, 'expected no error running pipeline');
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
