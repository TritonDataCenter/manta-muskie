/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var common = require('../common');

function loadRequest(req, res, next) {

    if (req.params.object_name) {
        req.bucketObject = new BucketObject(req);
    }

    if (req.params.bucket_name) {
        req.bucket = new Bucket(req);
    }

    next();

}

/* This is a function used before bucket object operations */
function checkBucketExists(req, res, next) {

    var log = req.log;
    if (req.bucketObject) {
        var bucketObject = req.bucketObject;
        log.debug({
            bucket_name: bucketObject.bucket_name,
            name: bucketObject.name
        }, 'checkBucketExists: requested');
    }

    var originalKey = req.key;
    /* libmanta's Moray helpers need this information to retrieve metadata */
    var opts = {
        /* This cuts the key down to /:account/buckets/:bucketname */
        key: bucketObject.bucket_name,
        owner: req.owner.account.uuid,
        requestId: req.getId()
    };

    req.moray.getBucket(opts, function onGetBucket(getErr, bucket_data) {
        if (getErr) {
            log.debug(getErr, 'getBucket: failed');
            next(getErr);
            return;
        }

        if (!bucket_data) {
            var mdErr = new ResourceNotFoundError(opts.key);
            log.debug({
                error: mdErr
            }, 'checkBucketExists: not found');
            next(mdErr);
            return;
        }

        log.debug({
            bucket: req.bucket
        }, 'checkBucketExists: done');

        req.bucket_data = bucket_data;
        next();
        // else {
        //     log.debug({
        //         bucket: bucket
        //     }, 'getBucket: done');
        //     res.send(200, bucket_data);
        //     next(null, bucket_data);
        // }
    });

    // common.loadMetadata(req, opts, function checkBucketMetadata(err, md) {

    //     if (err) {
    //         log.debug(err, 'checkBucketMetadata: failed');
    //         next(err);
    //         return;
    //     }

    //     if (!md || md.type === null) {
    //         var mdErr = new ResourceNotFoundError(opts.key);
    //         log.debug({
    //             error: mdErr
    //         }, 'checkBucketMetadata: not found');
    //         next(mdErr);
    //         return;
    //     }

    //     req.key = originalKey;
    //     req.bucket.mtime = new Date(md.mtime).toISOString();

    //     log.debug({
    //         bucket: req.bucket
    //     }, 'checkBucketMetadata: done');
    //     /*
    //      * The callback only needs the metadata to exist at all in order to
    //      * proceed, it does not need the contents of the metadata.
    //      */
    //     next();

    // });

}

function Bucket(req) {

    var self = this;

    assert.object(req, 'req');
    if (req.params.bucket_name) {
        self.name = req.params.bucket_name;
    }
    self.type = 'bucket';

    return (self);

}

function BucketObject(req) {

    var self = this;

    assert.object(req, 'req');
    assert.string(req.params.bucket_name, 'req.params.bucket_name');
    self.bucket_name = req.params.bucket_name;
    if (req.params.object_name) {
        self.name = req.params.object_name;
    }
    self.type = 'bucketobject';

    return (self);

}

module.exports = {
    Bucket: Bucket,
    BucketObject: BucketObject,
    checkBucketExists: checkBucketExists,
    loadRequest: loadRequest
};
