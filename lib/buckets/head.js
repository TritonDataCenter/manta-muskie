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
var common = require('../common');
var errors = require('../errors');

function headBucket(req, res, next) {

    var owner = req.owner.account.uuid;
    var bucket = req.bucket;
    var requestId = req.getId();
    var log = req.log;

    log.debug({
        owner: owner,
        bucket: bucket.name,
        requestId: requestId
    }, 'headBucket: requested');

    var onGetBucket = function onGet(err, bucket_data) {
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
            }, 'headBucket: failed');

            next(notFoundErr);
            return;
        } else {
            log.debug({
                owner: owner,
                bucket: bucket.name,
                requestId: requestId
            }, 'headBucket: done');

            next();
        }
    };

    req.boray.client.getBucketNoVnode(owner, bucket.name, onGetBucket);
}

function sendResponse(req, res, next) {
    var owner = req.owner.account.uuid;
    var bucket = req.bucket;
    var log = req.log;

    log.debug({
        owner: owner,
        bucket: bucket.name
    }, 'sendResponse: entered');

    res.send(200);

    log.debug({
        owner: owner,
        bucket: bucket.name
    }, 'sendResponse: done');

    next();
}

module.exports = {

    headBucketHandler: function headBucketHandler() {
        var chain = [
            buckets.loadRequest,
            headBucket,
            auth.authorizationHandler(),
            sendResponse
        ];
        return (chain);
    }

};
