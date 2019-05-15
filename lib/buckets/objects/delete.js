/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var buckets = require('../buckets');
var errors = require('../../errors');

function deleteObject(req, res, next) {
    var owner = req.owner.account.uuid;
    var bucket = req.bucket;
    var bucketObject = req.bucketObject;
    var requestId = req.getId();
    var log = req.log;

    log.debug({
        owner: owner,
        bucket: bucket.name,
        bucket_id: bucket.id,
        object: bucketObject.name,
        requestId: requestId
    }, 'deleteBucketObject: requested');

    var onObjectDelete = function onDelete(err, object_data) {
        var notFoundErr;
        if (err) {
            if (err.cause.name === 'ObjectNotFoundError') {
                notFoundErr = new errors.ObjectNotFoundError(bucketObject.name);
            } else {
                notFoundErr = err;
            }

            log.debug({
                err: notFoundErr,
                owner: owner,
                bucket: bucket.name,
                bucket_id: bucket.id,
                object: bucketObject.name,
                requestId: requestId
            }, 'deleteObject: error deleting object');

            next(notFoundErr);
            return;
        } else {
            log.debug({
                owner: owner,
                bucket: bucket.name,
                bucket_id: bucket.id,
                object: bucketObject.name,
                requestId: requestId
            }, 'deleteObject: done');
            res.send(204, null);
            next(null, object_data);
        }
    };

    req.boray.client.deleteObjectNoVnode(owner, bucket.id, bucketObject.name,
        onObjectDelete);
}

module.exports = {

    deleteBucketObjectHandler: function deleteBucketObjectHandler() {
        var chain = [
            buckets.loadRequest,
            buckets.getBucketIfExists,
            deleteObject
        ];
        return (chain);
    }

};
