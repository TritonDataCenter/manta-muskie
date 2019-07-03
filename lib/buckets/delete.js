/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var auth = require('../auth');
var buckets = require('./buckets');
var common = require('./common');
var errors = require('../errors');

function deleteBucket(req, res, next) {

    var owner = req.owner.account.uuid;
    var bucket = req.bucket;
    var log = req.log;
    var requestId = req.getId();

    log.debug({
        owner: owner,
        bucket: bucket.name
    }, 'deleteBucket: entered');

    var onDeleteBucket = function onDelete(err, bucket_data) {
        if (err) {
            log.debug({
                err: err,
                owner: owner,
                bucket: bucket.name
            }, 'deleteBucket: failed');

            next(err);
        } else {
            log.debug({
                owner: owner,
                bucket: bucket.name
            }, 'deleteBucket: done');

            res.send(204, null);
            next(null, bucket_data);
        }
    };

    var onGetBucket = function onGet(err1, bucket_data) {
        if (err1) {
            log.debug({
                err: err1,
                owner: owner,
                bucket: bucket.name
            }, 'getBucket: failed');

            next(err1);
            return;
        } else {
            // There is only one error type returned by this RPC
            if (bucket_data.error && bucket_data.error.name ===
                'BucketNotFound') {
                req.resource_exists = false;
                var not_found_error =
                    new errors.BucketNotFoundError(bucket.name);
                next(not_found_error);
            } else {
                log.debug({
                    owner: owner,
                    bucket: bucket.name
                }, 'getBucket: done');

                // The bucket exists, now check if it is empty
                var mreq = common.listObjects(req, bucket_data.id);
                var bucketEmpty = true;

                mreq.once('error', function onError(err2) {
                    mreq.removeAllListeners('end');
                    mreq.removeAllListeners('entry');

                    log.debug(err2, 'deleteBucket: empty bucket check failed');
                    next(err2);
                });

                mreq.once('entry', function onEntry(bucketObject) {
                    // Bucket is not empty so notify client immediately
                    mreq.removeAllListeners('end');
                    mreq.removeAllListeners('entry');
                    var notFoundErr =
                        new errors.BucketNotEmptyError(bucket.name);
                    bucketEmpty = false;
                    res.send(409, notFoundErr);
                    return (next(notFoundErr));
                });

                mreq.once('end', function onEnd() {
                    log.debug({}, 'deleteBucket: empty bucket check done');
                    if (bucketEmpty === true) {
                        req.boray.client.deleteBucketNoVnode(owner, bucket.name,
                            requestId, onDeleteBucket);
                    } else {
                        next();
                    }
                });
            }
        }
    };

    req.boray.client.getBucketNoVnode(owner, bucket.name, requestId,
        onGetBucket);
}


module.exports = {

    deleteBucketHandler: function deleteBucketHandler() {
        var chain = [
            buckets.loadRequest,
            auth.authorizationHandler(),
            deleteBucket
        ];
        return (chain);
    }

};
