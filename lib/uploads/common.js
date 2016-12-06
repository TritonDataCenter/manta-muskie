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
 *
 *  API OVERVIEW:
 *
 *  The Manta multipart upload API allows clients to upload a Manta object
 *  by splitting it into parts and uploading the parts individually. When all
 *  parts are uploaded, the client signifies that the upload is completed by
 *  "committing" the upload through the API, which creates a Manta object
 *  that is the sum of the uploaded parts and is indistiguisable from an
 *  object created through a normal Manta PUT. If a client decides not to
 *  finish the upload, it may also abort the upload process.
 *
 *  The possible operations in the mulitpart upload API are:
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
 *  TERMS:
 *
 *  There is some terminology that is used consistently throughout the
 *  multipart upload implementation that is useful to know:
 *
 *   - Upload ID: a uuid representing a multipart upload request, selected
 *     when the upload is created.
 *
 *   - Upload path: The path where parts of an upload are uploaded to and
 *     stored in Manta.
 *
 *   - Upload record: The Manta directory record for the upload path. This
 *     record contains state about the upload recording in an additional
 *     "upload" blob tacked onto the record. The upload record gives us a
 *     mechanism of passing state about an upload across various upload
 *     requests.
 *
 *   - Object path: An input to creating an upload, this refers to the path
 *     the object (created from the uploaded parts) will be stored at in
 *     Manta.
 *
 *   - Object record: The Manta object record for the upload's object path.
 *
 *   - Finalizing record: A Manta record (stored in a different
 *     bucket than object and directory metadata -- namely, "manta_uploads")
 *     and on the same shard as the object record. This upload is identified
 *     by both the object path and upload ID (both of which are used to
 *     construct the key used to insert the record into Moray). The presence of
 *     a finalizing record for a given object path and upload id indicates that
 *     either a commit or abort has begun for the upload. The finalizing record
 *     stores which type the record is, the upload ID and the etags for the
 *     parts.
 *
 *
 *  METADATA STRUCTURE:
 *
 *  Because most of the state about an upload is stored in metadata records in
 *  Moray, it is important to have a well-defined structure for what this
 *  information looks like.
 *
 *   - Upload record: This record has the same structure as a typical Manta
 *     directory record, with an additional object called "upload" that has
 *     the following structure:
 *
 *          upload {
 *              id,             // upload id
 *              state,          // state of the upload: CREATED or FINALIZING
 *              type,           // if state is FINALIZING, then ABORT or COMMIT
 *              objectPath,     // object path
 *              uploadPath,     // upload path
 *              headers,        // headers to store on object record
 *              sharks,         // mako sharks the object is stored on
 *              parts,          // when commit has started, etags of each part
 *              objectId        // object ID for the uploaded object
 *          }
 *
 *    - Finalizing record: This record has the same structure as a typical Manta
 *     directory record, with an additional object called "upload" that has
 *     the following structure:
 *          upload {
 *              id,             // upload id
 *              type,           // ABORT or COMMIT
 *              parts,          // when type is COMMIT, etags of each part
 *              objectPath      // object path
 *          }
 *
 *    - Object record: The object record is a normal Manta object record, but
 *      there are a few fields on the object that are set explitily by the
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
 *        - contentMD5: This is set either by the user when creating the
 *          upload, or set to a default value.
 *        - headers: This is set by the user when creating the upload.
 *        - sharks: These are selected when the upload is created.
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
 *
 */


///--- Globals

var sprintf = util.format;

var ID_REGEX = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
var PREFIX_LENGTH = 1;

// Upload states
var states = {
    CREATED: 'created',
    FINALIZING: 'finalizing'
};

// Finalizing types
var types = {
    COMMIT: 'commit',
    ABORT: 'abort'
};

// Used to lookup values in a loaded upload record
var mdKeys = {
    STATE: 'state',
    TYPE: 'type',
    OBJECT_PATH: 'objectPath',
    HEADERS: 'headers',
    SHARKS: 'sharks',
    PARTS: 'parts',
    OBJECT_ID: 'objectId'
};



///--- Helpers

/*
 * Creates the upload record for the upload path.
 * (e.g., /jhendricks/uploads/c/c46ac2b1-fcc3-4e12-8c46-c935808ed59f)
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
            //md._etag = null;
            md.upload = {
                id: upload.id,
                state: states.CREATED,
                type: null,  // used only for finalizing uploads
                objectPath: opts.objectPath,
                uploadPath: upload.uploadPath,
                headers: opts.headers,
                sharks: opts.sharks,
                parts: null, // used only for commits
                objectId: libuuid.create()
            };

            cb(null, md);
        }
    });
}

/*
 * Creates the metadata for the finalizing record for the upload, for both
 * commit and aborts.
 *
 * Parameters:
 *  - upload: MultipartUpload object
 *  - type: finalizing type
 *  - md5: if a commit, the md5 sum of the object
 *  - cb: function that is passed the metadata blob
 */
function createFinalizingRecord(upload, type, md5, cb) {
    assert.ok(type === types.COMMIT || type === types.ABORT);
    assert.func(cb);

    var req = upload.req;

    var md = {
        uploadId: upload.id,
        finalizingType: type,
        owner: req.owner.account.uuid,
        requestId: req.getId(),
        objectPath: upload.get(mdKeys.OBJECT_PATH),
        objectId: upload.get(mdKeys.OBJECT_ID),
        md5: md5,
        _etag: null
    };

    cb(md);
}


/*
 * Creates the object record.
 *
 * Parameters:
 *  - upload: MultipartUpload object
 *  - size: size of the object
 *  - md5: md5 sum (calculated in mako-finalize)
 *  - cb: function that is passed an error and the metadata blob
 */

function createObjectRecord(upload, size, md5, cb) {
    var req = upload.req;
    var objPath = upload.get(mdKeys.OBJECT_PATH);

    normalize(upload.req, objPath, function (err, objKey) {
        if (err) {
            cb(err);
        } else {
            //TODO: comment
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
                    // a few values need to be overriden.
                    md.objectId = upload.get(mdKeys.OBJECT_ID);
                    md.contentLength = size;
                    md.contentMD5 = md5;

                    var ct = upload.get(mdKeys.HEADERS)['content-type'];
                    if (ct) {
                        md.contentType = ct;
                    } else {
                        md.contentType = 'application/octet-stream';
                    }

                    md.sharks = upload.get(mdKeys.SHARKS);
                    //TODO: _etag.

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
    assert.ok(state === states.CREATED || state === states.FINALIZING);
    assert.func(cb);
    if (state === states.CREATED) {
        assert.ok(!type);
    } else {
        assert.ok(type === types.COMMIT || type === types.ABORT);
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

    upload.req.moray.getMetadata(options, function (err, md) {
        if (err) {
            cb(err);
        } else {
            assert.ok(md);
            assert.ok(md.upload, '\"upload\" not present in upload record md');

            record.loaded = md;
            record.toSave = jsprim.deepCopy(record.loaded);

            cb(null, record.loaded.upload);
        }
    });
}

/*
 * Loads the finalizing record.
 */
function loadFinalizingMetadata(upload, cb) {
    var record = upload.finalizingMd;

    function load(u, key, r, lcb) {
        assert.ok(key, 'key');

        r.key = key;
        var options = {
            key: key,
            requestId: upload.req.getId()
        };

        upload.req.moray.getFinalizingMetadata(options, function (err, md) {
            if (err) {
                lcb(err);
            } else {
                assert.ok(md);
                r.loaded = md;
                lcb(null, r.loaded);
            }
        });
    }

    var k = upload._objectPathKey;
    if (!k) {
        var o = upload.get(mdKeys.OBJECT_PATH);
        assert.ok(o, 'no object path found');
        normalize(upload.req, o, function (err, key) {
            if (err)  {
                cb(err);
            } else {
                upload._objectPathKey = key;
                load(upload, upload.constructKey(), record, cb);
            }
        });
    } else {
        load(upload, upload.constructKey(), record, cb);
    }
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
 * more simulataneous uploads.
 *
 * For example, for the input id '0bb83e47-32df-4833-a6fd-94d77e8c7dd3' and a
 * prefix length of 1, this function will return '0'.
 */
function idToPrefix(id) {
    assert.string(id);
    assert.ok(id.match(ID_REGEX));

    return (id.substring(0, PREFIX_LENGTH));
}


///--- Routes

function setupUpload(req, res, next) {
    var id = req.params.id;
    req.upload = new MultipartUpload(req, id);

    next();
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

    self._headers = null;
    self._size = null;
    self._copies = null;
    self._md5 = null;

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
            req.log.info('upload path key: ' + self.uploadMd.key);
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
    assert.ok(opts.sharks);
    assert.object(opts.headers);

    var self = this;

    self.objectPath = opts.objectPath;
    createUploadRecord(self, opts, function (err, uploadMd) {
        if (err) {
            cb(err);
        } else {
                    /* BEGIN JSSTYLED */
                    /* if (exists) {
                        uploadMd.etag = upload.etag;
                    } else {
                        uploadMd.etag = null;
                    } */
                    /* END JSSTYLED */

            self.uploadMd.toSave = uploadMd;
            persistUploadRecord(self, states.CREATED, null, function (err2) {
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
            if (type === types.ABORT) {
                log.info('abort record exists for upload ' + self.id);
                cb();
            } else {
                cb(new MultipartUploadFinalizeConflictError(upload.id,
                    types.ABORT));
            }

        } else {
            log.info('upload ' + self.id + ' has no finalizing record yet');
            createFinalizingRecord(self, types.ABORT, null, function (md) {
                var record = self.finalizingMd;
                record.toSave = md;

                self.req.log.info('saving finalizing record: ' + md);
                self.req.moray.putFinalizingMetadata(record.key,
                    record.toSave, function (err2) {
                    if (err2) {
                        cb(err2);
                    } else {
                        cb();
                    }
                });
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
function commitUpload(partsArr, size, md5, cb) {
    assert.ok(partsArr);
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
            if (type === types.ABORT) {
                cb(new MultipartUploadFinalizeConflictError(upload.id,
                    types.ABORT));
            } else {
                if (!jsprim.deepEqual(self.get(mdKeys.PARTS), partsArr)) {
                    cb(new MultipartUploadFinalizeConflictError(self.id,
                        types.COMMIT));
                } else {
                    log.info('valid commit record already exists for upload ' +
                        self.id);
                    cb();
                }
            }
        } else {
            createObjectRecord(self, size, md5, function (err2, objectMd) {
                if (err2) {
                    cb(err2);
                } else {
                    createFinalizingRecord(self, types.COMMIT, md5,
                    function (finalizingMd) {
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
                            key: self.constructKey(),
                            value: objectMd,
                            operation: 'put'
                        } ];
                        var opts = {
                            req_id: self.req.getId()
                        };

                        log.info('batch created: '  + JSON.stringify(batch));
                        self.req.moray.client.batch(batch, opts,
                        function (err3, meta) {
                            if (err3) {
                                log.error('error batching data: ' + err);
                                cb(err3);
                            } else {
                                log.info('batch successful');
                                 cb();
                            }
                        });
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
    loadUploadRecord(self, function (err, md) {
        if (err) {
            cb(err);
        } else {
            //XXX: Other useful things to include here?
            var upload = {
                id: self.id,
                uploadPath: self.uploadPath,
                objectPath: md.objectPath,
                state: md.state,
                // TODO: this is useful for debugging, but it may be something
                // we don't want to expose to outside clients.
                sharks: md.sharks,
                headers: md.headers,
                copies: md.copies
            };

            if (md.type === types.FINALIZING) {
                upload.type = md.type;
                upload.parts = md.parts;
            }

            if (md.contentMD5 !== '') {
                upload.contentMD5 = md.contentMD5;
            }

            //cb(null, upload);
            cb(null, md);
        }
    });
};


///--- Common methods for API endpoints

/*
 * Loads the metadata for the upload and returns its current state
 * and finalizing type, if applicable.
 */
MultipartUpload.prototype.uploadState = function uploadState(cb) {
    var log = this.req.log;
    var self = this;

    loadUploadRecord(self, function (err, upload) {
        if (err) {
            cb(err);
        } else {
            log.info(sprintf('loaded upload record for %s: state %s',
                self.id, upload.state));
            cb(null, upload.state, upload.type);
        }
    });
};


/*
 * Attemtps to load the upload's upload record, and if it exists,
 * passes the callback the record.
 */
MultipartUpload.prototype.uploadRecordExists = function uploadRecordExists(cb) {
    loadUploadRecord(this, function (err, upload) {
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
 * Attemtps to load the upload's finalizing record, and if it exists,
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
function finalizeUploadRecord(type, parts, cb) {
    assert.ok(type === types.COMMIT || type === types.ABORT);
    assert.ok(this.uploadMd.loaded, 'upload record not loaded');

    this.set(mdKeys.PARTS, parts);
    persistUploadRecord(this, states.FINALIZING, type, cb);
};


/*
 * Used by API handlers to set an item in the upload record.
 * The input key should be one of the keys specifeid in mdKeys.
 */
MultipartUpload.prototype.set = function set(k, v) {
    assert.ok(this.uploadMd.toSave);

    this.uploadMd.toSave.upload[k] = v;
};


/*
 * Looks up a value in the loaded upload record.
 */
MultipartUpload.prototype.get = function get(k) {
    assert.ok(this.uploadMd.loaded);

    return (this.uploadMd.loaded.upload[k]);
};


/*
 * Returns the size of the object if specifed on create, or a default value.
 */
MultipartUpload.prototype.uploadSize = function uploadSize() {
    assert.ok(this.uploadMd.loaded);
    var u = this.uploadMd.loaded.upload;
    assert.ok(u);

    var size = parseInt((u.headers['content-length'] || obj.DEF_MAX_LEN), 10);
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

    return (u.sharks.length);
};


/*
 * Used to create the key for the batch request to moray on commit.
 * The key is of the form: <upload id>:<object path>
 */
MultipartUpload.prototype.constructKey = function constructKey() {
    var o = this._objectPathKey;
    assert.ok(o);

    var key = this.id + ':' + o;
    this.req.log.info('key: ' + key);
    return (key);
};


// Returns the key for the upload path (for use in Moray).
MultipartUpload.prototype.uploadPathKey = function uploadPathKey() {
    var k = null;
    if (this.uploadMd) {
        k = this.uploadMd.key;
    }
    assert.ok(k);
    return (k);
};


///--- Exports

module.exports = {

    ID_REGEX: ID_REGEX,

    mdKeys: mdKeys,
    uploadStates: states,
    uploadTypes: types,

    MultipartUpload: MultipartUpload,

    setupUpload: setupUpload
};
