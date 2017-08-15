/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var util = require('util');

var assert = require('assert-plus');
var jsprim = require('jsprim');
var libmanta = require('libmanta');
var libuuid = require('libuuid');
var path = require('path');
var verror = require('verror');

var common = require('../common');
var obj = require('../obj');
require('../errors');


/*
 *  This file contains the majority of the logic for handling multipart uploads.
 *
 *  API OVERVIEW:
 *
 *  The Manta multipart upload API allows clients to upload a Manta object
 *  by splitting it into parts and uploading the parts individually. When all
 *  parts are uploaded, the client signifies that the upload is completed by
 *  "committing" the upload through the API, which creates a Manta object
 *  that is the concatenation of the uploaded parts and is indistinguishable
 *  from an object created through a normal Manta PUT. If a client decides not
 *  to finish the upload, it may also abort the upload process.
 *
 *  The possible operations in the multipart upload API are:
 *      - create: establish a multipart upload
 *      - upload-part: upload a part of the object
 *      - abort: cancel the upload
 *      - commit: complete the upload
 *      - get: get information about an ongoing upload
 *
 *  There is an additional API endpoint designed for client usability purposes
 *  that redirects all requests sent to the path /:account/upload/:id to the
 *  correct upload path.
 *
 *
 *  TERMINOLOGY:
 *
 *  There is some terminology that is used consistently throughout the
 *  multipart upload implementation that is useful to know:
 *
 *   - Upload ID: a uuid representing a multipart upload request, selected
 *     when the upload is created.
 *
 *   - Upload Path: The path where parts of an upload are uploaded to and
 *     stored in Manta. This is different from the target object path.
 *
 *   - Upload Record: The Manta directory record for the upload path. This
 *     record contains state about the upload recording in an additional
 *     "upload" blob tacked onto the record. The upload record gives us a
 *     mechanism of passing state about an upload across muskie requests.
 *
 *   - Target Object: The object that is intended to be created when the upload
 *     is committed.
 *
 *   - Target Object Path: An input to creating an upload, this refers to the
 *     path in Manta where the target object will be stored once the upload is
 *     committed.
 *
 *   - Target Object Record: The Manta object record for the target object.
 *
 *   - Finalizing Record: A Manta record, stored in a special multipart uploads
 *     bucket (namely, the "manta_uploads" bucket), that is guaranteed to exist
 *     on the same shard as the target object record. The key of this record is
 *     constructed using both the target object path and the upload ID. (By
 *     constructing the key this way, we allow for different multipart uploads
 *     for the same target object path, whether they are simultaneous or not.)
 *     The presence of a finalizing record for a given target object path and
 *     upload ID pair indicates that an upload has been "finalized" -- either
 *     committed or aborted.
 *
 *  METADATA STRUCTURE:
 *
 *  Because most of the state about an upload is stored in metadata records in
 *  Moray, it is important to have a well-defined structure for what this
 *  information looks like.
 *
 *   - Upload Record: This record has the same structure as a typical Manta
 *     directory record, with an additional object called "upload" that has
 *     the following structure:
 *
 *      upload {
 *          id,             // upload id
 *          state,          // state of the upload: CREATED or FINALIZING
 *          type,           // if state is FINALIZING, then ABORT or COMMIT
 *          objectPath,     // target object path
 *          objectPathKey,  // the Moray key for the target object path
 *          uploadPath,     // upload path
 *          headers,        // headers to store on object record
 *          sharks,         // mako sharks the object is stored on
 *          partsMD5,       // for a commit, the MD5 sum of the parts etags
 *          objectId,       // object ID for the target object
 *          creationTimeMs  // upload record creation time in ms since epoch
 *      }
 *
 *    - Finalizing Record: This record has the same structure as a typical Manta
 *     directory record, with an additional object called "upload" that includes
 *     the following fields:
 *      upload {
 *          uploadId,           // upload id
 *          finalizingType,     // ABORT or COMMIT
 *          objectPath          // target object path
 *          objectId            // target object id
 *          md5                 // MD5 sum of the object (returned from mako)
 *      }
 *
 *    - Object Record: The object record is a normal Manta object record, but
 *      there are a few fields on the object that are set explicitly by the
 *      multipart upload code, instead of the common metadata code.
 *
 *      In particular, the following fields are set explicitly:
 *        - objectId: This is generated when the object is created and is
 *          needed for mako-finalize.
 *        - contentLength: This is set either by the user when creating the
 *          upload (and validated by the commit endpoint), or it is
 *          calculated on commit.
 *        - contentMD5: This is set either by the user when creating the
 *          upload (and validated by the commit endpoint), or it is
 *          calculated on commit.
 *        - headers: This is set by the user when creating the upload.
 *        - sharks: These are selected when the upload is created.
 *
 *
 *  AUTHORIZATION:
 *
 *  Generally, MPU actions are authorized against the resource /:account/uploads
 *  (for MPU creation) or /:account/uploads/[0-f]/:id (other MPU actions).
 *  Subusers are not allowed to perform multipart uploads.
 *
 *
 *  IMPLEMENTATION DETAILS:
 *
 *  The logic of this API is implemented as methods on the MultipartUpload
 *  object defined in this file. When a multipart upload related request comes
 *  in to muskie, a new MultipartUpload request is constructed, and sets up
 *  some state the various handlers will need.
 *
 *  After an upload's creation, most of the state about the upload is stored in
 *  the upload record. After validating inputs to a request, the first thing a
 *  multipart upload API endpoint should do is call the method uploadState(),
 *  which will load the upload record from Moray and allow the handlers to
 *  fetch state from the record using the get() method, and modify the record
 *  using the set() method.
 *
 *  Once the API handlers have completed the relevant logic based on the
 *  upload's state, they each call a relevant method on the upload object
 *  ({create,abort,commit,get}Upload()). These methods take care of saving
 *  the upload record back to Moray, and perform any additional metadata
 *  transformations as needed.
 */


///--- Globals

var sprintf = util.format;
var hasKey = jsprim.hasKey;

// Regex of an upload id (which is just a uuid).
var ID_REGEX = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
var PART_NUM_REGEX = /^([0-9]|[1-9][0-9]{0,3})$/;

/*
 * Multipart upload parts are stored in the directory:
 *   /:account/uploads/<prefix directory>/:id.
 *
 * The purpose of the prefix directory is to prevent us from hitting the
 * max number of directory entries within the "uploads" folder. As such,
 * we want to divide uploads roughly evenly into subdirectories.
 * Because uuids are randomly generated, we expect that they would be uniformly
 * distributed based on the first PREFIX_LENGTH characters of the upload id.
 *
 * For now, we use a PREFIX_LENGTH of 1, so /:account/uploads has a maximum
 * of 16 directories, which allows for 16 million ongoing multipart uploads.
 * If needed, we can bump this prefix length to allow for more.
 */
var PREFIX_LENGTH = 1;

// Range of allowed part numbers is [0, 10000).
var MIN_PART_NUM = 0;
var MAX_PART_NUM = 9999;
var MAX_NUM_PARTS = MAX_PART_NUM - MIN_PART_NUM + 1;

// Upload states
var MPU_S_CREATED = 'created';
var MPU_S_FINALIZING = 'finalizing';
var MPU_S_DONE = 'done';

// Finalizing types
var MPU_FT_COMMIT = 'commit';
var MPU_FT_ABORT = 'abort';

// These are the keys used to look up values in the MPU upload record.
// (See MultipartUpload.get() and MultipartUpload.set()).
var mdKeys = {
    // the state of the upload
    STATE: 'state',

    // if state is FINALIZING, the finalizing type
    TYPE: 'type',

    // target object path
    OBJECT_PATH: 'objectPath',

    // the normalized target object path for Manta, which includes the owner
    // uuid instead of the account name. It is the key used to insert/look up
    // the target object path in Moray.
    OBJECT_PATH_KEY: 'objectPathKey',

    // path to the upload directory (where parts are logically stored)
    UPLOAD_PATH: 'uploadPath',

    // headers specified for the target object when the MPU was created
    HEADERS: 'headers',

    // sharks selected for the target object; where parts are uploaded
    SHARKS: 'sharks',

    // for a committed object, the md5 sum of all part etags concatenated,
    // in order (with no characters between them)
    PARTS_MD5: 'partsMD5',

    // the target object ID
    OBJECT_ID: 'objectId',

    // upload record creation time
    CREATION_TIME_MS: 'creationTimeMs'
};

function isValidUploadMdKey(key) {
    var consts = Object.keys(mdKeys);

    for (var i = 0; i < consts.length; i++) {
        if (mdKeys[consts[i]] === key) {
            return (true);
        }
    }

    return (false);
}



///--- Helpers

/*
 * Creates an upload record in memory for the upload path.
 * (e.g., /jhendricks/uploads/c/c46ac2b1-fcc3-4e12-8c46-c935808ed59f)
 *
 * To save the record in Moray, use MultipartUpload.persistUploadRecord().
 *
 * Parameters:
 *  - upload: MultipartUpload object
 *  - opts: options blob that must have the following items:
 *      - objectPath
 *      - sharks
 *      - headers
 *  - cb: function that is passed an error and the metadata blob
 */
function createUploadRecord(upload, opts, cb) {
    assert.object(opts);
    assert.string(opts.objectPath);
    assert.string(opts.objectPathKey);
    assert.object(opts.sharks);
    assert.object(opts.headers);
    assert.string(upload.uploadPath);
    assert.string(upload.id);
    assert.func(cb);

    var req = upload.req;

    /*
     * createMetadata assumes that the key for the metadata it should create
     * is saved in req.key, which is true for most requests. In this case,
     * we save the key stored in req.key (which corresponds to the path
     * /:account/uploads, instead of the upload record key) and restore it
     * after the metadata is created. This is a bit janky, but allows us to
     * reuse existing code to create metadata for directories much more
     * easily.
     */
    var savedKey = req.key;
    assert.ok(upload.uploadMd.key);
    req.key = upload.uploadMd.key;

    common.createMetadata(req, 'directory', function (err, md) {
        req.key = savedKey;

        if (err) {
            cb(err);
        } else {
            md._etag = null;
            md.upload = {
                id: upload.id,
                state: MPU_S_CREATED,
                type: null,  // used only for finalizing uploads
                objectPath: opts.objectPath,
                objectPathKey: opts.objectPathKey,
                uploadPath: upload.uploadPath,
                headers: opts.headers,
                sharks: opts.sharks,
                partsMD5: null, // used only for commits
                objectId: libuuid.create(),
		creationTimeMs: Date.now()
            };

            cb(null, md);
        }
    });
}

/*
 * Creates the metadata object in memory for an MPU finalizing record, for both
 * commit and aborts.
 *
 * Parameters:
 *  - upload: MultipartUpload object
 *  - type: finalizing type
 *  - md5: if a commit, the md5 sum of the object
 */
function createFinalizingRecord(upload, type, md5) {
    assert.ok(type === MPU_FT_COMMIT || type === MPU_FT_ABORT);
    if (type === MPU_FT_COMMIT) {
        assert.string(md5);
    }

    var req = upload.req;

    var md = {
        uploadId: upload.id,
        finalizingType: type,
        owner: req.owner.account.uuid,
        requestId: req.getId(),
        objectPath: upload.get(mdKeys.OBJECT_PATH),
        objectId: upload.get(mdKeys.OBJECT_ID),
        md5: md5,

        // This is critical for finalizing records, as it tells Moray we only
        // want to insert the record if none already exists for the same key.
        // If we didn't do this, we might clobber another running finalizing
        // record update instead of failing.
        _etag: null
    };

    return (md);
}


/*
 * Creates the metadata for the target object record in memory.
 *
 * Parameters:
 *  - upload: MultipartUpload object
 *  - size: size of the object
 *  - md5: md5 sum (calculated in mako-finalize)
 *  - cb: function that is passed an error and the metadata blob
 */

function createTargetObjectRecord(upload, size, md5, cb) {
    var req = upload.req;
    var objPath = upload.get(mdKeys.OBJECT_PATH);

    normalize(upload.req, objPath, function (err, objKey) {
        if (err) {
            cb(err);
        } else {
            // createMetadata relies on some values hanging of the request
            // object, so we save and restore these before calling it.
            var savedKey = req.key;
            var savedHeaders = req.headers;

            req.key = objKey;
            req.headers = upload.get(mdKeys.HEADERS);
            req.query.metadata = null;

            common.createMetadata(req, 'object', function (err2, md) {
                if (err2) {
                    cb(err2);
                } else {
                    req.key = savedKey;
                    req.headers = savedHeaders;

                    // createMetadata does most of the work for us here, but
                    // a few values need to be overwritten.
                    md.objectId = upload.get(mdKeys.OBJECT_ID);
                    md.contentLength = size;
                    md.contentMD5 = md5;

                    var ct = upload.get(mdKeys.HEADERS)['content-type'];
                    if (ct) {
                        md.contentType = ct;
                    } else {
                        md.contentType = 'application/octet-stream';
                    }

                    /*
                     * Since the size of the target object isn't always known
                     * when the MPU is created, it's possible we selected
                     * sharks for a zero-byte object. We don't want to
                     * include sharks for a zero-byte object in its object
                     * record, though.
                     */
                    if (size !== 0) {
                        md.sharks = upload.get(mdKeys.SHARKS);
                    } else {
                        md.sharks = [];
                    }

                    cb(null, md);
                }
            });
        }
    });
}


/*
 * Saves the upload record to moray with the given state and type.
 *
 * Parameters:
 *  - upload: MultipartUpload object
 *  - state: upload state
 *  - type: if applicable, finalizing type
 *  - cb: function
 */
function persistUploadRecord(upload, state, type, cb) {
    assert.ok(state === MPU_S_CREATED || state === MPU_S_FINALIZING);
    assert.func(cb);
    if (state === MPU_S_CREATED) {
        assert.ok(!type);
        assert.ok(upload.uploadMd.toSave._etag === null);
    } else {
        assert.ok(type === MPU_FT_COMMIT || type === MPU_FT_ABORT);
        assert.ok(upload.uploadMd.toSave._etag);
    }
    assert.ok(upload.uploadMd.toSave, 'no upload record to save');

    var log = upload.req.log;

    upload.set(mdKeys.STATE, state);
    upload.set(mdKeys.TYPE, type);

    upload.uploadMd.toSave.requestId = upload.req.getId();
    upload.req.moray.putMetadata(upload.uploadMd.toSave, function (err, md) {
        if (err) {
            cb(err);
        } else {
            log.debug({
                uploadId: upload.id,
                record: md
            }, 'persistUploadRecord: done');
            cb();
        }
    });
}


/*
 * Loads the upload record, saves a copy of it, and creates a new
 * copy that can be modified throughout the request.
 */
function loadUploadRecord(upload, cb) {
    var record = upload.uploadMd;

    var options = {
        key: record.key,
        requestId: upload.req.getId()
    };

    upload.req.moray.getMetadata(options, function (err, md, wrap) {
        if (err) {
            cb(err);
        } else {
            assert.ok(md);
            assert.ok(md.upload, '\"upload\" not present in upload record md');

            record.loaded = md;
            record.toSave = jsprim.deepCopy(record.loaded);

            assert.ok(wrap._etag);
            record.toSave._etag = wrap._etag;

            cb(null, record.loaded.upload);
        }
    });
}

/*
 * Loads the finalizing record.
 */
function loadFinalizingMetadata(upload, cb) {
    var record = upload.finalizingMd;

    record.key = upload.constructKey();
    var options = {
        key: record.key,
        requestId: upload.req.getId()
    };

    upload.req.moray.getFinalizingMetadata(options, function (err, md) {
        if (err) {
            cb(err);
        } else {
            assert.ok(md);
            record.loaded = md;
            cb(null, record.loaded);
        }
    });
}


// Normalizes a path in Manta.
function normalize(req, mPath, cb) {
    var opts = {
        account: req.owner.account,
        path: mPath
    };

    libmanta.normalizeMantaPath(opts, function (err, p) {
        if (err) {
            req.log.debug({
                url: path,
                err: err
            }, 'failed to normalize URL');
            cb(err);
        } else {
            cb(null, p);
        }
    });
}


/*
 * Given an upload ID, returns the prefix to use for the parent directory
 * of the upload directory. For now, this is the just the first character of
 * the upload uuid, but we may want to use more characters later to allow for
 * more simultaneous uploads.
 *
 * For example, for the input id '0bb83e47-32df-4833-a6fd-94d77e8c7dd3' and a
 * prefix length of 1, this function will return '0'.
 */
function idToPrefix(id) {
    assert.string(id);
    assert.ok(id.match(ID_REGEX), 'upload ID does not match uuid regex');

    return (id.substring(0, PREFIX_LENGTH));
}


///--- Routes

/*
 * Common handler used by all API calls on an existing multipart upload
 * that loads the upload record, which is used in subsequent handlers,
 * and stores information about the upload at req.upload.
 */
function loadUpload(req, res, next) {
    var log = req.log;

    /*
     * Multipart upload is not supported for subusers. For now, if the
     * caller of this operation is a subuser of an account, we disallow
     * any use of the multipart upload API.
     */
    if (req.caller.user) {
        next(new AuthorizationError(req.caller.user.login, req.url));
        return;
    }

    var id = req.params.id;
    if (!id.match(ID_REGEX)) {
        next(new ResourceNotFoundError('upload ID ' + id));
        return;
    }

    req.upload = new MultipartUpload(req, id);
    loadUploadRecord(req.upload, function (err, upload) {
        if (err) {
            next(err);
        } else {
            log.debug(sprintf('loaded upload record for %s: state %s',
                req.upload.id, upload.state));
            next();
        }
    });
}


/*
 * This handler sets up the authContext that is eventually passed to mahi to
 * authorize the user to perform an action on an existing multipart upload.
 * It is the analog to "storageContext" for storage-based actions, or
 * "jobContext" in the jobs API.
 */
function uploadContext(req, res, next) {
    var log = req.log;
    log.debug('uploadContext: started');

    var opts = {
        // keys like /<account uuid>/uploads/[0-f]/<upload id>
        key: req.key.split('/').slice(0, 4).join('/'),
        requestId: req.getId()
    };

    common.loadMetadata(req, opts, function (md_err, md) {
        if (md_err) {
            next(md_err);
            return;
        }
        var o = req.upload.uploadOwner();
        req.mahi.getAccountById(o, function (auth_err, owner) {
            if (auth_err) {
                next(auth_err);
                return;
            }

            if (owner.account.uuid !== req.owner.account.uuid) {
                next(new ResourceNotFoundError(req.path()));
                return;
            }

            req.authContext.resource = {
                owner: owner,
                key: md.key || req.key,
                roles: md.roles || []
            };

            log.debug('uploadContext: completed');
            next();
        });
    });
}


///--- API

/*
 * Constructor for the MultipartUpload object, which is instantiated at
 * the beginning of each multipart upload related request and attached to the
 * request object at `req.upload`.
 *
 * The inputs to the constructor are:
 *      - id, the upload uuid
 *      - req, the request object for this multipart upload related request
 *
 *
 * The structure of this object is as follows:
 *
 * {
 *    id,                     // upload uuid
 *    req,                    // pointer to the request this upload is for
 *    uploadPath,             // upload path
 *
 *
 *    // Private fields used to share state across a specific upload request.
 *    // They aren't always used or set.
 *    _headers,
 *    _size,
 *    _copies,
 *    _md5,
 *
 *
 *    // These objects represent some of the relevant metadata for the upload.
 *
 *    // When an upload or finalizing record is first loaded during a request,
 *    // it is saved on the MultipartUpload object at
 *    // `{upload,finalizing}Md.loaded`. Changes to metadata are made in a copy
 *    // of the metadata that is saved at {upload,finalizing}Md.toSave.
 *
 *    // Additionally, each object contains the moray bucket and the key
 *    // for the metadata record it represents.
 *
 *    uploadMd {          // upload record metadata object
 *        key,            // normalized uploadPath
 *        bucket,         // bucket for upload records (normal manta records)
 *        loaded {        // current metadata for this upload
 *        toSave          // new metadata for this upload
 *    },
 *
 *    finalizingMd {      // finalizing record metadata object
 *        key,            // normalized objectPath
 *        bucket,         // bucket for finalizing records
 *        loaded,         // current metadata for this upload
 *        toSave          // new metadata for this upload
 *    }
 * }
 *
 */
function MultipartUpload(req, id) {
    var self = this;
    self.id = id;
    self.req = req;
    self.uploadPath = '/' + req.owner.account.login + '/uploads/' +
        idToPrefix(id) + '/' + id;

    self.mpuHeaders = null;
    self.mpuSize = null;
    self.mpuCopies = null;
    self.mpuContentMD5 = null;
    self.mpuPartsMD5 = null;
    self.mpuObjectPathKey = null;

    self.uploadMd = {
        key: null,
        bucket: 'manta',
        loaded: null,
        toSave: null
    };

    normalize(req, self.uploadPath, function (err, p) {
        if (err) {
            throw (new InvalidPathError(self.uploadPath));
        } else {
            self.uploadMd.key = p;
        }
    });

    self.finalizingMd = {
        key: null,
        bucket: 'manta_uploads',
        loaded: null,
        toSave: null
    };

    return (self);
}


///--- Create

/*
 * Creates the multipart upload by creating the upload record and inserting
 * it into Moray.
 *
 * Parameters:
 *  - opts: options blob that expects:
 *      - objectPath
 *      - sharks: array of shark objects returned from the picker
 *      - headers: user-specified headers object (or an empty object)
 */
MultipartUpload.prototype.createUpload = function createUpload(opts, cb) {
    assert.func(cb);
    assert.string(opts.objectPath);
    assert.string(opts.objectPathKey);
    assert.ok(opts.sharks);
    assert.object(opts.headers);

    var self = this;

    createUploadRecord(self, opts, function (err, uploadMd) {
        if (err) {
            cb(err);
        } else {
            self.uploadMd.toSave = uploadMd;
            persistUploadRecord(self, MPU_S_CREATED, null, function (err2) {
                if (err2) {
                    cb(err2);
                } else {
                    cb(null, self.uploadPath);
                }
            });
        }
    });
};




/*
 * Aborts an upload.
 *
 * First validates that no commit record exists for the upload, then inserts
 * an abort record for the upload on the object shard.
 */
MultipartUpload.prototype.abortUpload = function abortUpload(cb) {
    var log = this.req.log;
    var self = this;

    self.finalizingRecordExists(function (err, exists, upload) {
        if (err) {
            cb(err);
        } else if (exists) {
            // This is only an error if the record isn't an abort record.
            var type = self.finalizingMd.loaded.finalizingType;
            if (type === MPU_FT_ABORT) {
                log.debug('abort record exists for upload ' + self.id);
                cb();
            } else {
                cb(new MultipartUploadInvalidArgumentError(upload.id,
                    'already aborted'));
            }

        } else {
            var md = createFinalizingRecord(self, MPU_FT_ABORT, null);
            var record = self.finalizingMd;
            record.toSave = md;

            self.req.moray.putFinalizingMetadata({
                key: record.key,
                md: record.toSave
            },
            function (err2) {
                if (err2) {
                    cb(err2);
                } else {
                    cb();
                }
            });
        }
    });
};


/*
 * Commits an upload.
 *
 * First checks for the existence of a finalizing record, then saves the
 * upload record as finalizing, and atomically inserts a commit record
 * and object record on the object's shard.
 */
MultipartUpload.prototype.commitUpload =
function commitUpload(partsMD5, size, md5, cb) {
    assert.string(partsMD5);
    assert.number(size);
    assert.string(md5);
    assert.func(cb);

    var log = this.req.log;
    var self = this;

    self.finalizingRecordExists(function (err, exists, upload) {
        if (err) {
            cb(err);
        } else if (exists) {
            // This is valid only for a commit record with matching parts.
            var type = self.finalizingMd.loaded.finalizingType;
            if (type === MPU_FT_ABORT) {
                cb(new MultipartUploadStateError(upload.id, 'already aborted'));
            } else {
                if (self.get(mdKeys.PARTS_MD5) !== partsMD5) {
                    cb(new MultipartUploadStateError(self.id,
                        'already committed with a different part set'));
                } else {
                    log.debug('valid commit record already exists for upload ' +
                        self.id);
                    cb();
                }
            }
        } else {
            createTargetObjectRecord(self, size, md5,
            function (err2, objectMd) {
                if (err2) {
                    cb(err2);
                } else {
                    var finalizingMd = createFinalizingRecord(self,
                        MPU_FT_COMMIT, md5);
                    var batch = [ {
                        bucket: self.finalizingMd.bucket,
                        key: self.finalizingMd.key,
                        value: finalizingMd,
                        operation: 'put',
                        opts: {
                            req_id: self.req.getId(),
                            etag: null
                        }
                    }, {
                        bucket: self.uploadMd.bucket,
                        key: self.get(mdKeys.OBJECT_PATH_KEY),
                        value: objectMd,
                        operation: 'put'
                    } ];
                    var opts = {
                        requestId: self.req.getId(),
                        requests: batch
                    };

                    self.req.moray.commitMPU(opts, function (err3, meta) {
                        if (err3) {
                            log.error('error batching data: ' + err3);
                            cb(err3);
                        } else {
                            log.debug('batch successful');
                            cb();
                        }
                    });
                }
            });
        }
    });
};

//--- Get
/*
 * Returns a object representation of the upload that can be serialized as JSON
 * and sent to the client.
 */
MultipartUpload.prototype.getUpload = function getUpload(cb) {
    var self = this;

    var upload = {
        id: self.id,
        state: self.get(mdKeys.STATE),
        partsDirectory: self.get(mdKeys.UPLOAD_PATH),
        targetObject: self.get(mdKeys.OBJECT_PATH),
        headers: self.get(mdKeys.HEADERS),
        numCopies: self.numSharks(),
	creationTimeMs: self.get(mdKeys.CREATION_TIME_MS)
    };

    if (upload.state === MPU_S_CREATED) {
        setImmediate(cb, null, upload);
    } else {
        assert.ok(upload.state === MPU_S_FINALIZING);
        self.finalizingRecordExists(function (err, exists, fr) {
            if (err) {
                cb(err, null);
            } else {
                if (exists) {
                    upload.state = MPU_S_DONE;
                    delete upload.partsDirectory;

                    if (fr.finalizingType === MPU_FT_COMMIT) {
                        upload.partsMD5Summary = self.get(mdKeys.PARTS_MD5);
                        upload.result = 'committed';
                    } else {
                        upload.result = 'aborted';
                    }
                } else {
                    // This means the upload has started to be finalized, but
                    // the finalizing record hasn't made it into Moray yet,
                    // either because another request to finalize the upload is
                    // processing, or a previous one failed.
                    upload.type = self.get(mdKeys.TYPE);
                }

                cb(null, upload);
            }
        });
    }
};


///--- Common methods for API endpoints

/*
 * Attempts to load the upload's finalizing record, and if it exists,
 * passes the callback the record. This is useful for both committing
 * and aborting uploads.
 */
MultipartUpload.prototype.finalizingRecordExists =
function finalizingRecordExists(cb) {
    loadFinalizingMetadata(this, function (err, upload) {
        if (err) {
            if (verror.hasCauseWithName(err, 'ObjectNotFoundError')) {
                cb(null, false);
            } else {
                cb(err);
            }
        } else {
            cb(null, true, upload);
        }
    });
};


/*
 * Saves the upload record with state set to FINALIZING.
 *
 * Parameters:
 *  - type: finalizing type
 *  - parts: if a commit, array of etags representing the parts
 *  - cb: function
 */
MultipartUpload.prototype.finalizeUploadRecord =
function finalizeUploadRecord(type, md5, cb) {
    assert.ok(type === MPU_FT_COMMIT || type === MPU_FT_ABORT);
    assert.ok(this.uploadMd.loaded, 'upload record not loaded');
    assert.ok(this.uploadMd.toSave._etag, 'no etag on upload record');

    this.set(mdKeys.PARTS_MD5, md5);
    persistUploadRecord(this, MPU_S_FINALIZING, type, cb);
};


/*
 * Used by API handlers to set an item in the upload record.
 * The input key should be one of the keys specified in mdKeys.
 */
MultipartUpload.prototype.set = function set(k, v) {
    assert.ok(this.uploadMd.toSave);
    assert.ok(isValidUploadMdKey(k));

    this.uploadMd.toSave.upload[k] = v;
};


/*
 * Looks up a value in the loaded upload record.
 */
MultipartUpload.prototype.get = function get(k) {
    assert.ok(this.uploadMd.loaded);
    assert.ok(isValidUploadMdKey(k));

    return (this.uploadMd.loaded.upload[k]);
};


/*
 * Returns the size of the object if specified on create, or a default value.
 */
MultipartUpload.prototype.uploadSize = function uploadSize() {
    assert.ok(this.uploadMd.loaded);
    var u = this.uploadMd.loaded.upload;
    assert.ok(u);

    var size;
    if (hasKey(u.headers, 'content-length')) {
        size = u.headers['content-length'];
    } else {
        size = obj.DEF_MAX_LEN;
    }
    assert.number(size);
    assert.ok(size >= 0);

    return (size);
};


/*
 * Verifies that if a size was specified on create, the input expected value
 * matches this size.
 */
MultipartUpload.prototype.checkSize = function checkSize(expected, cb) {
    assert.ok(this.uploadMd.loaded);
    var u = this.uploadMd.loaded.upload;
    assert.ok(u);

    if (!u.headers['content-length']) {
        cb(true);
    } else {
        var size = parseInt(u.headers['content-length'], 10);
        assert.ok(size >= 0);
        if (size !== expected) {
            cb(false, size);
        } else {
            cb(true);
        }
    }
};

/*
 * Verifies that if an md5 was specified on create, the input expected value
 * matches this md5.
 */
MultipartUpload.prototype.checkMD5 = function checkMD5(expected, cb) {
    assert.ok(this.uploadMd.loaded);
    var u = this.uploadMd.loaded.upload;
    assert.ok(u);

    var md5 = u.headers['content-md5'];

    if (!md5) {
        cb(true);
    } else {
        if (md5 !== expected) {
            cb(false, md5);
        } else {
            cb(true);
        }
    }
};


/*
 * Returns the number of sharks selected for this upload on create.
 */
MultipartUpload.prototype.numSharks = function numSharks() {
    assert.ok(this.uploadMd.loaded);
    var u = this.uploadMd.loaded.upload;
    assert.ok(u);
    assert.arrayOfObject(u.sharks);

    return (u.sharks.length);
};


/*
 * Used to create the key for the batch request to moray on commit.
 * The key is of the form: <upload id>:<object path>
 */
MultipartUpload.prototype.constructKey = function constructKey() {
    var o = this.get(mdKeys.OBJECT_PATH_KEY);
    assert.ok(o, 'objectPathKey not saved in upload');

    var key = this.id + ':' + o;
    return (key);
};


// Returns the key for the upload path (for use in Moray).
MultipartUpload.prototype.uploadPathKey = function uploadPathKey() {
    assert.ok(this.uploadMd);
    var k = this.uploadMd.key;
    assert.ok(k);
    return (k);
};


// Returns the owner of the upload.
MultipartUpload.prototype.uploadOwner = function uploadOwner() {
    assert.ok(this.uploadMd.loaded, 'upload record not loaded');
    assert.ok(this.uploadMd.loaded.owner, 'no owner found in upload record');
    assert.string(this.uploadMd.loaded.owner, 'owner is not a string');

    return (this.uploadMd.loaded.owner);
};


///--- Exports

module.exports = {

    ID_REGEX: ID_REGEX,
    PART_NUM_REGEX: PART_NUM_REGEX,
    MIN_PART_NUM: MIN_PART_NUM,
    MAX_PART_NUM: MAX_PART_NUM,
    MAX_NUM_PARTS: MAX_NUM_PARTS,
    MPU_S_CREATED: MPU_S_CREATED,
    MPU_S_FINALIZING: MPU_S_FINALIZING,
    MPU_FT_ABORT: MPU_FT_ABORT,
    MPU_FT_COMMIT: MPU_FT_COMMIT,

    mdKeys: mdKeys,

    MultipartUpload: MultipartUpload,

    // Common handlers for API endpoints
    loadUpload: loadUpload,
    uploadContext: uploadContext
};
