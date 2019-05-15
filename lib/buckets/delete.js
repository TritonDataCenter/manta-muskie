/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var buckets = require('./buckets');

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
        // TODO: Return proper error in the case where the bucket does not exist
        if (err) {
            log.debug({
                err: err,
                owner: owner,
                bucket: bucket.name,
                requestId: requestId
            }, 'deleteBucket: error reading bucket data');
            next(err);
            return;
        } else {
            log.debug({
                owner: owner,
                bucket: bucket.name,
                data: bucket_data,
                requestId: requestId
            }, 'deleteBucket: done');
            res.send(204, null);
            next();
        }
    };

    req.boray.client.deleteBucketNoVnode(owner, bucket.name, onDeleteBucket);
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
