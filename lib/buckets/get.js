/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var buckets = require('./buckets');
var common = require('../common');
var errors = require('../errors');

function getBucket(req, res, next) {

    var owner = req.owner.account.uuid;
    var bucket = req.bucket;
    var requestId = req.getId();
    var log = req.log;

    log.debug({
        owner: owner,
        bucket: bucket.name,
        requestId: requestId
    }, 'getBucket: requested');

    var onGetBucket = function onGet(err, bucket_data) {
        var notFoundErr;
        if (err) {
            if (err.cause.name === 'BucketNotFoundError') {
                notFoundErr = new errors.BucketNotFoundError(bucket.name);
            } else {
                notFoundErr = err;
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
            res.send(200, bucket);
            next(null, bucket_data);
        }
    };

    req.boray.getBucketNoVnode(owner, bucket.name, onGetBucket);
}

module.exports = {

    getBucketHandler: function getBucketHandler() {
        var chain = [
            buckets.loadRequest,
            getBucket
        ];
        return (chain);
    }

};
