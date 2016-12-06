/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var jsprim = require('jsprim');
var crypto = require('crypto');
var restify = require('restify');
var path = require('path');
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');

var auth = require('../auth');
var common = require('../common');
var obj = require('../obj');
var sharkClient = require('../shark_client');
var uploadsCommon = require('./common');
var utils = require('../utils');
require('../errors');


///--- Globals

var hasKey = jsprim.hasKey;
var shallowCopy = utils.shallowCopy;
var sprintf = util.format;

/*
 * We enforce a minimum part size because each part requires additional space
 * and load on the metadata tier throughout the life of the upload: uploading
 * parts, contacting Moray to validate etags on commit, and performing garbage
 * collection of the parts and their metadata after the upload is finalized.
 *
 * Thus, we discourage excessive use of parts by requiring all parts (except
 * the last part) to be at least 5 MB.
 */
var MIN_PART_SIZE = 5242880;


///--- Helpers

/*
 * Invokes the mako-finalize operation on a single shark.
 *
 * Parameters:
 * - req the current request
 * - body: body to POST to the shark
 * - opts: an options blob to pass to the shark client
 * - shark: a shark object
 */
function invokeMakoFinalize(req, body, opts, shark, cb) {
    var sharkOpts = {
        connectTimeout: req.sharkConfig.connectTimeout,
        log: req.log,
        retry: req.sharkConfig.retry,
        shark: shark,
        agent: req.sharkAgent
    };
    var client = sharkClient.getClient(sharkOpts);
    assert.ok(client, 'sharkClient returned null');

    var start = Date.now();
    var hostname = shark.manta_storage_id;

    client.post(opts, body, function (err, _, res) {
        /*
         * Similar to PUTs, log information about sharks we contacted over
         * the course of the commit request.
         */
        var sharkInfo = {
            shark: hostname,
            timeTotal: Date.now() - start,
            result: 'fail',
            _startTime: start
        };
        req.sharksContacted.push(sharkInfo);

        var s = {
            shark: hostname,
            md5: null
        };
        if (err) {
            cb(err, s);
        } else {
            s.md5 = res.headers['x-joyent-computed-content-md5'];
            if (!s.md5) {
                var cause = new Error('mako failed to return an MD5 sum');
                cb(new InternalError(cause));
            } else {
                sharkInfo.result = 'ok';
                cb(null, s);
            }
        }
    });
}


// Given an array of etags, computes the md5 sum of all etags concatenated.
function computePartsMD5(parts) {
    var hash = crypto.createHash('md5');
    parts.forEach(function (p) {
        hash.update(p);
    });

    return (hash.digest('base64'));
}


///--- API


// Get the owner of the resource of the input object path.
function loadOwner(req, res, next) {
    var p = req.upload.get(uploadsCommon.mdKeys.OBJECT_PATH);
    auth.loadOwnerFromPath(req, p, next);
}


/*
 * For authorizing the use of the object path on commit, we rely on
 * existing handlers for other muskie endpoints: in particular,
 * getMetadata, which loads the metadata for the parent directory, if
 * needed, and the metadata for the object at the path stored at req.key; and
 * storageContext, with sets up the authContext used by mahi to authorize
 * the caller to perform the given action. In order to use these handlers,
 * we need to set some state on the request object, specifically:
 *      - req.key: the key of the object path (for this commit)
 *      - req.parentKey: the key of the object path's parent directory
 */
function setupMetadataState(req, res, next) {
    var log = req.log;

    req._path = req.upload.get(uploadsCommon.mdKeys.OBJECT_PATH);

    req.key = req.upload.get(uploadsCommon.mdKeys.OBJECT_PATH_KEY);
    if (!req.isRootDirectory(req.key)) {
        req.parentKey = path.dirname(req.key);
    }

    log.debug({
        objectPathKey: req.key,
        parentKey: req.parentKey
    }, 'passing keys to getMetadata');

    next();
}


/*
 * Ensures that the upload is in a proper state before proceeding: either
 * CREATED or COMMIT. It it is in state COMMIT, the request must specify the
 * same set of parts as is recorded in the upload record.
 */
function validateUploadState(req, res, next) {
    var log = req.log;
    var parts = req.body.parts || [];
    var id = req.upload.id;

    var state = req.upload.get(uploadsCommon.mdKeys.STATE);
    var type = req.upload.get(uploadsCommon.mdKeys.TYPE);
    var computedPartsMD5 = computePartsMD5(parts);

    req.upload.mpuPartsMD5 = computedPartsMD5;

    log.debug({
        uploadId: id,
        parts: parts,
        uploadState: state,
        finalizingType: type ? type : 'N/A',
        parts: parts,
        computedPartsMD5: computedPartsMD5
    }, 'commit: requested');

    if (state === uploadsCommon.MPU_S_FINALIZING) {
        if (type === uploadsCommon.MPU_FT_ABORT) {
            // Abort already in progress
            next(new MultipartUploadStateError(id, 'already aborted'));

        } else if (type === uploadsCommon.MPU_FT_COMMIT) {
            // Not an error, but we need to verify the parts are the
            // same as the input ones before proceeding
            var p = req.upload.get(uploadsCommon.mdKeys.PARTS_MD5);
            if (computedPartsMD5 !== p) {
                log.info({
                    userSpecified: computedPartsMD5,
                    expected: p
                }, 'mismatch of parts md5');

                next(new MultipartUploadStateError(id,
                    'already has a commit in progress'));
            } else {
                next();
            }

        } else {
            assert.fail('Invalid type: ' + type);
        }
    } else if (state === uploadsCommon.MPU_S_CREATED) {
        next();
    } else {
        assert.fail('Invalid state: ' + state);
    }
}


/*
 * Ensures that the input parts set for the commit has the etags of the parts
 * as they exist now. This step also checks that all parts have a size that
 * exceeds the minimum part size (excluding the last part).
 */
function validateParts(req, res, next) {
    var log = req.log;
    var id = req.upload.id;
    var parts = req.body.parts;
    log.debug('validating parts for upload');

    if (!parts) {
        req.body.parts = [];
        log.debug('empty parts array');
        next();
        return;
    } else if (parts.length > uploadsCommon.MAX_NUM_PARTS) {
        next(new MultipartUploadInvalidArgumentError(id,
            sprintf('too many parts provided (%d provided; max is %d)',
                parts.length, uploadsCommon.MAX_NUM_PARTS)));
        return;
    }

    var errors = [];
    var sum = 0;

    /*
     * This function verifies that:
     * - the etag exists
     * - the etag matches the current etag for the part
     * - the size of the part is at least the minimum size, unless
     *   it's the last part
     */
    function validateEtag(part, cb) {
        var index = part.index;
        var etag = part.etag;

        var record = req.upload.uploadMd;
        var key = record.key + '/' + index;

        if (etag === '') {
            errors.push(new MultipartUploadInvalidArgumentError(id,
                sprintf('[part %d] no etag provided', index, etag)));
            cb();
            return;
        }

        var opts = {
            key: key,
            requestId: req.getId()
        };

        req.moray.getMetadata(opts, function (err, md) {
            log.debug('part index: ' + index + ', etag: ' + etag);
            if (err) {
                if (verror.hasCauseWithName(err, 'ObjectNotFoundError')) {
                    //  No part with that part number has been uploaded.
                    errors.push(new MultipartUploadInvalidArgumentError(id,
                        sprintf('[part %d] part does not exist', index)));
                } else {
                    errors.push(new InternalError(err));
                }
            } else {
                var size = parseInt(md.contentLength, 10);
                var isFinalPart = index === (parts.length - 1);

                if (md.etag !== etag) {
                    // Uploaded part has a different etag than the input one.
                    errors.push(new MultipartUploadInvalidArgumentError(id,
                        sprintf('[part %d] invalid etag "%s"', index, etag)));
                } else if (!isFinalPart && (size < MIN_PART_SIZE)) {
                    errors.push(new MultipartUploadInvalidArgumentError(id,
                        sprintf('[part %d] part is too small (size %d)',
                            index, size)));
                }

                sum += size;
            }

            cb();
        });
    }

    var queue = vasync.queue(validateEtag, 10);
    parts.forEach(function (val, i) {
        queue.push({
            index: i,
            etag: val
        });
    });
    queue.close();

    queue.on('end', function () {
        log.debug('part validation completed');
        if (errors.length > 0) {

            // Even if there's an internal error, we still send any user
            // errors we encountered, so they can be corrected and retried.
            for (var i = 0; i < errors.length; i++) {
                var e = errors[i];
                assert.number(e.statusCode);
                if (e.statusCode >= 500) {
                    log.error('internal error: ' + e);
                } else if (e.statusCode >= 400) {
                    next(e);
                    return;
                } else {
                    assert.fail('invalid error: ' + e);
                }
            }

            next(new InternalError(new Error('commit error')));

        } else {
            req.upload.checkSize(sum, function (valid, expected) {
                if (!valid) {
                    next(new MultipartUploadInvalidArgumentError(id,
                        sprintf('expected content-length %s, but object ' +
                            'was %s', expected, sum)));
                } else {
                    req.upload.mpuSize = sum;
                    next();
                }
            });
        }
    });
}


/*
 * Saves the upload record with its state set to FINALIZING.
 */
function finalizingState(req, res, next) {
    assert.ok(req.upload.mpuPartsMD5);

    req.upload.finalizeUploadRecord(
        uploadsCommon.MPU_FT_COMMIT,
        req.upload.mpuPartsMD5,
        function (err) {
            if (err) {
                next(err);
            } else {
                next();
            }
    });
}


/*
 * Invokes the mako-finalize operation on each shark selected at the beginning
 * of the upload. If there is a problem with any of the sharks, this operation
 * will fail.
 *
 * The mako node expects a JSON blob of the form:
 * {
 *      version,        // the version of multipart upload this is
 *      owner,          // string uuid of the owner of the upload object
 *      nbytes,         // expected size of the object
 *      objectId,       // string uuid of object
 *      parts,          // array of string uuids for each part
 * }
 *
 * This handler is also expected to set the following on the uploads object:
 *  - contentMD5
 *  - objectId (if it does not exist yet)
 */
function finalizeUpload(req, res, next) {
    var log = req.log;

    var id = req.upload.id;
    var objectId = req.upload.get(uploadsCommon.mdKeys.OBJECT_ID);
    var sharks = req.upload.get(uploadsCommon.mdKeys.SHARKS);
    var nbytes = req.upload.mpuSize;

    // Skip mako-finalize for zero-byte uploads.
    if (nbytes === 0) {
        log.debug('zero-byte object; skipping mako-finalize');

        // Verify the md5 sum, if provided.
        var h = req.upload.get(uploadsCommon.mdKeys.HEADERS);
        if (hasKey(h, 'content-md5')) {
            var m = h['content-md5'];
            if (m !== obj.ZERO_BYTE_MD5) {
                next(new MultipartUploadInvalidArgumentError(id,
                    sprintf('expected content-md5 "%s", but object ' +
                        'has md5 "%s"', obj.ZERO_BYTE_MD5, m)));
            }
        }

        req.upload.mpuContentMD5 = obj.ZERO_BYTE_MD5;
        req.upload.mpuSize = 0;
        next();
        return;
    }

    var body = {
        version: 1,
        nbytes: nbytes,
        account: req.owner.account.uuid,
        objectId: objectId,
        parts: req.body.parts
    };

    var opts = {
        objectId: objectId,
        owner: req.owner.account.uuid,
        requestId: req.getId(),
        path: '/mpu/v1/commit/' + id
    };

    req.sharksContacted = [];

    vasync.forEachParallel({
        func: function finalize(shark, cb) {
            var _opts = shallowCopy(opts);
            var _body = shallowCopy(body);
            invokeMakoFinalize(req, _body, _opts, shark, cb);
        },
        inputs: sharks
    },  function (err, results) {
            log.debug('mako-finalize: all sharks contacted');

            if (err) {
                results.operations.forEach(function (r) {
                    log.error('error with shark ' + r.result.shark +
                        ': ' + r.err);
                });
                next(new SharksExhaustedError());
            } else {
                var md5 = null;

                // Validate that all makos returned the same md5 sum.
                var mismatch = false;
                results.operations.forEach(function (r) {
                    assert.ok(r.status === 'ok');
                    assert.ok(r.result);

                    if (md5 && (md5 !== r.result.md5)) {
                        mismatch = true;
                    } else {
                        md5 = r.result.md5;
                    }
                });

                if (mismatch) {
                    var msg = 'mako nodes returned different md5 sums for ' +
                        'the same object';

                    log.error({
                        results: results.operations.map(function (r) {
                            return ({
                                shark: r.result.shark,
                                md5: r.result.md5
                            });
                    })}, msg);

                    next(new InternalError(new Error(msg)));
                } else {
                    // Validate user-provided md5 sums.
                    req.upload.checkMD5(md5, function (valid, expected) {
                        if (!valid) {
                            next(new MultipartUploadInvalidArgumentError(id,
                                sprintf('expected content-md5 "%s", but ' +
                                'object has md5 "%s"', expected, md5)));
                        } else {
                            req.upload.mpuContentMD5 = md5;
                            next();
                        }
                    });
                }
            }
    });
}


/*
 * This step makes the committed upload visible from Manta by atomically
 * inserting a commit record and object record on the shard associated
 * with the object. Most of the heavy lifting is done by the req.uploads
 * object here.
 */
function commit(req, res, next) {
    var log = req.log;

    var size = req.upload.mpuSize;
    var md5 = req.upload.mpuContentMD5;
    var partsMD5 = req.upload.mpuPartsMD5;

    assert.number(size);
    assert.string(md5);

    req.upload.commitUpload(partsMD5, size, md5, function (err) {
        if (err) {
            next(err);
        } else {
            var p = req.upload.get(uploadsCommon.mdKeys.OBJECT_PATH);

            log.info({
                uploadId: req.upload.id,
                objectPath: p,
                size: size,
                numParts: req.body.parts.length,
                partsMD5: partsMD5
            }, 'commit: completed');

            res.setHeader('Location', p);
            res.send(201);
            next();
        }
    });
}

function ensureNotRoot(req, res, next) {
    var p = req.upload.get(uploadsCommon.mdKeys.OBJECT_PATH);

    if (!req.isRootDirectory(p)) {
        next();
        return;
    } else {
        next(new RootDirectoryError(req.method, p));
    }
}


///--- Exports

module.exports = {
    commitHandler: function commitHandler() {
        var chain = [
            uploadsCommon.loadUpload,
            uploadsCommon.uploadContext,
            auth.authorizationHandler(),
            loadOwner,
            setupMetadataState,
            common.getMetadataHandler(),
            auth.storageContext,
            auth.authorizationHandler(),
            common.ensureNotDirectoryHandler(),
            common.ensureParentHandler(),
            ensureNotRoot,
            obj.enforceDirectoryCountHandler(),
            restify.jsonBodyParser({
                mapParams: false,
                maxBodySize: 500000
            }),
            validateUploadState,
            validateParts,
            finalizingState,
            finalizeUpload,
            commit
        ];
        return (chain);
    }
};
