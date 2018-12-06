/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var buckets = require('../buckets');
var common = require('../../common');
var obj = require('../../obj');

function getBucketObject(req, res, next) {

    var bucketObject = req.bucketObject;
    var log = req.log;
    log.debug({
        bucket_name: bucketObject.bucket_name,
        name: bucketObject.name
    }, 'getBucketObject: requested');

    /* libmanta's Moray helpers need this information to retrieve metadata */
    var opts = {
        key: req.key,
        requestId: req.getId()
    };

    common.loadMetadata(req, opts, function getBucketObjectMetadata(loadErr, md)
    {

        if (loadErr) {
            log.debug({
                key: opts.key,
                metadata: md.value,
                requestId: md.requestId
            }, 'getBucketObjectMetadata: failed');
            next(loadErr);
            return;
        }

        if (!md || md.type === null) {
            var mdErr = new ResourceNotFoundError(opts.key);
            log.debug({
                error: mdErr
            }, 'getBucketObjectMetadata: not found');
            next(mdErr);
            return;
        }

        req.metadata = md;
        req.metadata.type = bucketObject.type;
        req.metadata.sharks = md.sharks;

        log.debug({
            key: opts.key,
            requestId: opts.requestId,
            metadata: req.metadata
        }, 'getBucketObject: done');
        next(null, md);

    });

}

module.exports = {

    getBucketObjectHandler: function getBucketObjectHandler() {
        var chain = [
            buckets.loadRequest,
            buckets.checkBucketExists,
            getBucketObject,
            /*
             * Call into the storage object file to access pre-existinge code
             * written to stream data from shards.
             */
            obj.getBucketObjectHandler()
        ];
        return (chain);
    }

};
