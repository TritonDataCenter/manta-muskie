/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var buckets = require('./buckets');
var errors = require('../errors');

function createBucket(req, res, next) {

    var owner = req.owner.account.uuid;
    var bucket = req.bucket;
    var requestId = req.getId();
    var log = req.log;

    log.debug({
        owner: owner,
        bucket: bucket.name,
        requestId: requestId
    }, 'createBucket: entered');

    var onCreateBucket = function onCreate(err, bucket_data) {
        if (bucket_data !== undefined && bucket_data._node) {
            // Record the name of the shard and vnode contacted.
            req.entryShard = bucket_data._node.pnode;
            req.entryVnode = bucket_data._node.vnode;
        }

        if (err) {
            var alreadyExistsErr;
            if (err.name === 'BucketAlreadyExistsError') {
                alreadyExistsErr = new errors.BucketExistsError(bucket.name);
            } else {
                alreadyExistsErr = err;
            }
            log.debug({
                err: alreadyExistsErr,
                owner: owner,
                bucket: bucket.name,
                requestId: requestId
            }, 'createBucket: error creating bucket');
            next(alreadyExistsErr);
        } else {
            log.debug({
                owner: owner,
                bucket: bucket.name,
                data: bucket_data,
                requestId: requestId
            }, 'createBucket: done');
            res.send(204, bucket_data);
            next(null, bucket_data);
        }
    };

    req.boray.client.createBucketNoVnode(owner, bucket.name, onCreateBucket);
}

module.exports = {

    createBucketHandler: function createBucketHandler() {
        var chain = [
            buckets.loadRequest,
            createBucket
        ];
        return (chain);
    }

};
