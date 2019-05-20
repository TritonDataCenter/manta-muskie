/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var buckets = require('./buckets');
var common = require('./common');
var errors = require('../errors');

function deleteBucket(req, res, next) {

    var owner = req.owner.account.uuid;
    var bucket = req.bucket;
    var requestId = req.getId();
    var log = req.log;
    log.debug({
        name: bucket.name
    }, 'deleteBucket: requested');

    log.debug({
        owner: owner,
        bucket: bucket.name,
        requestId: requestId
    }, 'deleteBucket: entered');

    var onDeleteBucket = function onDelete(err, bucket_data) {
        var notFoundErr;
        if (err) {
            if (err.name === 'BucketNotFoundError') {
                notFoundErr = new errors.BucketNotFoundError(bucket.name);
            } else {
                notFoundErr = err;
            }

            log.debug({
                err: notFoundErr,
                owner: owner,
                bucket: bucket.name,
                requestId: requestId
            }, 'deleteBucket: failed');

            next(notFoundErr);
            return;
        } else {
            log.debug({
                owner: owner,
                bucket: bucket.name,
                requestId: requestId
            }, 'deleteBucket: done');
            res.send(204, null);
            next(null, bucket_data);
        }
    };

    var onGetBucket = function onGet(err1, bucket_data) {
        var notFoundErr;
        if (err1) {
            if (err1.name === 'BucketNotFoundError') {
                notFoundErr = new errors.BucketNotFoundError(bucket.name);
            } else {
                notFoundErr = err1;
            }

            log.debug({
                err: notFoundErr,
                owner: owner,
                bucket: bucket.name,
                requestId: requestId
            }, 'getBucket: failed');

            next(notFoundErr);
            return;
        } else {
            log.debug({
                owner: owner,
                bucket: bucket.name,
                requestId: requestId
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
                notFoundErr = new errors.BucketNotEmptyError(bucket.name);
                bucketEmpty = false;
                res.send(409, notFoundErr);
                return (next(notFoundErr));
            });

            mreq.once('end', function onEnd() {
                log.debug({}, 'deleteBucket: empty bucket check done');
                if (bucketEmpty === true) {
                    req.boray.client.deleteBucketNoVnode(owner, bucket.name,
                        onDeleteBucket);
                } else {
                    next();
                }
            });



        }
    };


    req.boray.client.getBucketNoVnode(owner, bucket.name, onGetBucket);
}


module.exports = {

    deleteBucketHandler: function deleteBucketHandler() {
        var chain = [
            buckets.loadRequest,
            deleteBucket
        ];
        return (chain);
    }

};
