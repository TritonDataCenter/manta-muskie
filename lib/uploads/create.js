/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var assert = require('assert-plus');
var jsprim = require('jsprim');
var libmanta = require('libmanta');
var libuuid = require('libuuid');
var path = require('path');
var restify = require('restify');
var util = require('util');
var verror = require('verror');
var Ajv = require('ajv');
var ajv = new Ajv({allErrors: true});

/*
 * All keywords provided by ajv-keywords must be explicitly imported here. Any
 * ajv-keywords used without being imported will be silently ignored during
 * validation.
 */
require('ajv-keywords')(ajv, ['prohibited']);

var auth = require('../auth');
var common = require('../common');
var obj = require('../obj');
var uploadsCommon = require('./common');
var muskieUtils = require('../utils');
require('../errors');


///--- Globals

var hasKey = jsprim.hasKey;
var sprintf = util.format;
var schemaValidator = ajv.compile({
    'type': 'object',
    'properties': {
        'objectPath': {
            'type': 'string',
            'minLength': 1
        },
        'headers': {
            'type': 'object',
            'patternProperties' : {
                'content-length' : {
                    'type': 'number',
                    'minimum': 0
                }
            },
            'prohibited': [
                'if-match',
                'if-none-match',
                'if-modified-since',
                'if-unmodified-since'
            ]
        }
    },
    'required': ['objectPath']
});

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

        req.authContext.resource = {
            owner: req.owner,
            key: md.key || req.key,
            roles: md.roles || []
        };

        next();
    });
}

function setupUpload(req, res, next) {
    assert.object(req.msk_defaults, 'req.msk_defaults');
    assert.number(req.msk_defaults.mpuPrefixDirLen,
        'req.msk_defaults.mpuPrefixDirLen');

    var prefixLen = req.msk_defaults.mpuPrefixDirLen;
    var id = uploadsCommon.newUploadId(prefixLen);

    var opts = {
        id: id,
        login: req.owner.account.login
    };
    var uploadPath = uploadsCommon.generateUploadPath(opts);
    req.upload = new uploadsCommon.MultipartUpload(id, uploadPath, req);
    next();
}

function validateSchema(req, res, next) {
    uploadsCommon.validateJsonSchema(schemaValidator, req.body,
    function (valid, msg) {
        if (valid) {
            next();
        } else {
            next(new MultipartUploadCreateError(msg));
        }
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
 *   - content-disposition
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

    var opts = {
        account: req.targetObjectOwner.account,
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

            // Supported headers are: content-length, {x-}durability-level,
            // and content-md5. We set these values to defaults otherwise.
            if (hasKey(headers, 'content-length')) {
                size = headers['content-length'];
            } else {
                size = req.msk_defaults.maxStreamingSize;
            }

            // Validation for the durability-level header is done here because
            // the case of an invalid durability level is given its own special
            // muskie error. To avoid redundancy in the json-validation logic,
            // we leave validation of the parameter as is.
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

            // validate the content-disposition header
            muskieUtils.validateContentDisposition(
                headers, function cb(cdErr, _h) {
                    if (cdErr) {
                        req.log.debug('malformed content-disposition: %s',
                                      cdErr.msg);
                        res.send(400);
                        next(false);
                    }
                });

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

function loadTargetObjectOwner(req, res, next) {
    var objectPath = req.body.objectPath;
    var account;
    try {
        account = decodeURIComponent(objectPath.split('/', 2).pop());
    } catch (e) {
        next(new MultipartUploadCreateError('Invalid objectPath' + objectPath));
        return;
    }


    // We don't support mpu for users yet.
    var user = common.ANONYMOUS_USER;
    var fallback = true;
    req.mahi.getUser(user, account, fallback, function (err, owner) {
        if (err) {
            switch (err.restCode || err.name) {
            case 'AccountDoesNotExist':
                next(new AccountDoesNotExistError(account));
                return;
            default:
                next(new InternalError(err));
                return;
            }
        }

        req.targetObjectOwner = owner;
        next();
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
            setupUpload,
            validateSchema,
            loadTargetObjectOwner,
            validateParams,
            ensurePrefixDir,
            createUpload
        ];
        return (chain);
    }
};
