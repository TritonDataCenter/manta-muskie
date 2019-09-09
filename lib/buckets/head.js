/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var auth = require('../auth');
var buckets = require('./buckets');
var common = require('../common');
var errors = require('../errors');

function headBucket(req, res, next) {

    var owner = req.owner.account.uuid;
    var bucket = req.bucket;
    var log = req.log;
    var requestId = req.getId();

    log.debug({
        owner: owner,
        bucket: bucket.name
    }, 'headBucket: requested');

    var onGetBucket = function onGet(err, bucket_data) {
        if (err) {
            log.debug({
                err: err,
                owner: owner,
                bucket: bucket.name
            }, 'headBucket: failed');

            next(err);
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
                }, 'headBucket: done');
                next();
            }
        }
    };

    req.boray.client.getBucketNoVnode(owner, bucket.name, requestId,
        onGetBucket);
}

module.exports = {

    headBucketHandler: function headBucketHandler() {
        var chain = [
            buckets.loadRequest,
            headBucket,
            auth.authorizationHandler(),
            buckets.successHandler
        ];
        return (chain);
    }

};
