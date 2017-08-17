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
var libmanta = require('libmanta');
var libuuid = require('libuuid');
var path = require('path');
var restify = require('restify');
var util = require('util');
var verror = require('verror');

var auth = require('../auth');
var common = require('../common');
var obj = require('../obj');
var uploadsCommon = require('./common');
require('../errors');


///--- Globals

var hasKey = jsprim.hasKey;
var sprintf = util.format;


///--- Helpers

/*
 * Selects the sharks for the upload through the picker.choose interface.
 *
 * The number of sharks needed and the size of the sharks are specified by
 * the durability-level and the content-length headers, respectively, or
 * set to a default value.
 */
function chooseSharks(req, size, copies, cb) {
    assert.object(req, 'req');
    assert.number(size, 'size');
    assert.number(copies, 'copies');
    assert.func(cb, 'callback');

    var log = req.log;

    if (size === 0) {
        setImmediate(cb, null, {});
    } else {
        var opts = {
            requestId: req.getId(),
            replicas: copies,
            size: size
        };
        req.picker.choose(opts, function (err, sharks) {
            if (err) {
                cb(err);
            } else {
                log.debug({
                    sharks: sharks[0]
                }, 'upload: sharks chosen');
                cb(null, sharks[0]);
            }
        });
    }
}


///--- API


/*
 * This sets up the req.authContext object, which the authorization code
 * will use to authorize the user. The resource the caller is trying to
 * access for upload creation is the top-level uploads directory.
 *
 * We also check at this point if the caller is a subuser, which is not
 * allowed for MPU at this time.
 */
function uploadContextRoot(req, res, next) {
    // Disallow subusers from creating uploads
    if (req.caller.user) {
        next(new AuthorizationError(req.caller.user.login, req.url));
        return;
    }

    var opts = {
        // Derive the top-level uploads directory (/<account uuid>/uploads).
        key: req.key.split('/').slice(0, 3).join('/'),
        requestId: req.getId()
    };

    common.loadMetadata(req, opts, function (err, md) {
        if (err) {
            next(err);
            return;
        }

        var id = libuuid.create();
        req.upload = new uploadsCommon.MultipartUpload(req, id);

        req.authContext.resource = {
            owner: req.owner,
            key: md.key || req.key,
            roles: md.roles || []
        };

        next();
    });
}


/*
 * Validates that all parameters needed for creating an upload exist, including:
 *   - objectPath (the final path the uploaded object resides)
 *
 * Also validates optional headers, if they exist:
 *   - durability-level
 *   - content-length
 *   - content-md5
 *
 * This handler is expected to set the following state on the upload object:
 * - objectPath
 * - size
 * - copies
 * - headers
 * - contentMD5
 * - contentType
 */
function validateParams(req, res, next) {
    assert.number(req.msk_defaults.maxStreamingSize, 'maxStreamingSize');

    var log = req.log;

    if (!req.body.objectPath || (typeof (req.body.objectPath) !== 'string')) {
        next(new MultipartUploadInvalidArgumentError('a valid "objectPath" ' +
            'is required'));
    } else {
        var opts = {
            account: req.owner.account,
            path: req.body.objectPath
        };
        libmanta.normalizeMantaPath(opts, function (err, p) {
            if (err) {
                log.debug({
                    url: path,
                    err: err
                }, 'failed to normalize URL');

                next(err);
            } else {
                var inputHeaders, headers, size, copies;

                inputHeaders = req.body.headers || {};
                var maxObjectCopies = req.config.maxObjectCopies ||
                    obj.DEF_MAX_COPIES;

                headers = {};
                Object.keys(inputHeaders).forEach(function (k) {
                    headers[k.toLowerCase()] = inputHeaders[k];
                });

                // Reject conditional headers.
                if (hasKey(headers, 'if-match') ||
                    hasKey(headers, 'if-none-match') ||
                    hasKey(headers, 'if-modified-since') ||
                    hasKey(headers, 'if-unmodified-since')) {
                    next(new MultipartUploadCreateError('conditional headers ' +
                        'are not supported for multipart upload objects'));
                    return;
                }

                // Supported headers are: content-length, {x-}durability-level,
                // and content-md5. We set these values to defaults otherwise.
                if (hasKey(headers, 'content-length')) {
                    size = headers['content-length'];
                    if (typeof (size) === 'number') {
                        if (size < 0) {
                            var msg = '"content-length" must be >= 0';
                            next(new MultipartUploadCreateError(msg));
                            return;
                        }
                    } else {
                        next(new MultipartUploadCreateError('"content-length"' +
                        ' must be a number'));
                        return;
                    }
                } else {
                    size = req.msk_defaults.maxStreamingSize;
                }

                if (hasKey(headers, 'durability-level')) {
                    copies = headers['durability-level'];
                } else if (hasKey(headers, 'x-durability-level')) {
                    copies = headers['x-durability-level'];
                } else {
                    copies = obj.DEF_NUM_COPIES;
                }

                if (typeof (copies) !== 'number' ||
                    copies < obj.DEF_MIN_COPIES || copies > maxObjectCopies) {
                    next(new InvalidDurabilityLevelError(obj.DEF_MIN_COPIES,
                        maxObjectCopies));
                    return;
                }

                assert.string(p);
                assert.object(headers);
                assert.number(size);
                assert.number(copies);

                req.upload.mpuObjectPathKey = p;
                req.upload.mpuHeaders = headers;
                req.upload.mpuSize = size;
                req.upload.mpuCopies = copies;

                log.debug({
                    objectPath: req.body.objectPath,
                    headers: headers,
                    size: size,
                    copies: copies
                }, 'create-mpu: requested');

                next();
            }
        });
    }
}


/*
 * Checks if the parent of the upload directory exists, and if it doesn't,
 * creates the directory.
 *
 * For example,if the prefix length for an upload ID is 1, and the id is abcdef,
 * the prefix directory is of the form: /account/uploads/a.
 */
function ensurePrefixDir(req, res, next) {
    var log = req.log;
    var requestId = req.getId();

    var parentOpts = {
        key: path.dirname(req.upload.uploadPathKey()),
        requestId: requestId
    };

    req.moray.getMetadata(parentOpts, function (err, md, _) {
        if (err) {
            if (verror.hasCauseWithName(err, 'ObjectNotFoundError')) {
                // If the directory doesn't exist yet, create it.
                parentOpts.dirname = path.dirname(parentOpts.key);
                parentOpts.mtime = Date.now();
                parentOpts.owner = req.owner.account.uuid;
                parentOpts.requestId = req.getId();
                parentOpts.type = 'directory';

                req.moray.putMetadata(parentOpts, function (err2) {
                    if (err2) {
                        next(err2);
                    } else {
                        log.debug('prefix directory \"' + parentOpts.key +
                            '\" created');
                        next();
                    }
                });
            } else {
                next(err);
            }
        } else {
            next();
        }
    });
}


/*
 * Actually create the upload in the sense that the upload record exists.
 * To do so, we must first choose the sharks that the final object will
 * live on and save the metadata for the upload record.
 */
function createUpload(req, res, next) {
    var log = req.log;

    var s = req.upload.mpuSize;
    var c = req.upload.mpuCopies;

    chooseSharks(req, s, c, function (err, sharks) {
        if (err) {
            next(err);
        } else {
            var opts = {
                objectPath: req.body.objectPath,
                objectPathKey: req.upload.mpuObjectPathKey,
                sharks: sharks,
                headers: req.upload.mpuHeaders
            };
            req.upload.createUpload(opts, function (err2, partsDirectory) {
                    if (err2) {
                        next(err2);
                    } else {
                        log.debug({
                            id: req.upload.id,
                            sharks: sharks
                        }, 'create-mpu: completed');

                        res.send(201, {
                            id: req.upload.id,
                            partsDirectory: partsDirectory
                        });
                        next();
                    }
            });
        }
    });
}


///--- Exports

module.exports = {
    createHandler: function createHandler() {
        var chain = [
            uploadContextRoot,
            auth.authorizationHandler(),
            restify.jsonBodyParser({
                mapParams: false,
                maxBodySize: 100000
            }),
            validateParams,
            ensurePrefixDir,
            createUpload
        ];
        return (chain);
    }
};
