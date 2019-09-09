/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var auth = require('../../auth');
var buckets = require('../buckets');
var conditional = require('../../conditional_request');
var errors = require('../../errors');

function deleteObject(req, res, next) {
    var owner = req.owner.account.uuid;
    var bucket = req.bucket;
    var bucketObject = req.bucketObject;
    var log = req.log;
    var requestId = req.getId();

    log.debug({
        owner: owner,
        bucket: bucket.name,
        bucket_id: bucket.id,
        object: bucketObject.name
    }, 'deleteBucketObject: requested');

    var onObjectDelete = function onDelete(err, object_data) {
        if (err) {
            log.debug({
                err: err,
                owner: owner,
                bucket: bucket.name,
                bucket_id: bucket.id,
                object: bucketObject.name
            }, 'deleteObject: error deleting object');

            next(err);
            return;
        } else {
            // There is only one error type returned by this RPC
            if (object_data.error && object_data.error.name ===
                'ObjectNotFound') {
                    req.resource_exists = false;
                    req.not_found_error =
                        new errors.ObjectNotFoundError(bucketObject.name);
                    next();
            } else {
                log.debug({
                    owner: owner,
                    bucket: bucket.name,
                    bucket_id: bucket.id,
                    object: bucketObject.name
                }, 'deleteObject: done');

                req.resource_exists = true;
                next(null, object_data);
            }
        }
    };

    req.boray.client.deleteObjectNoVnode(owner, bucket.id, bucketObject.name,
        requestId, onObjectDelete);
}

module.exports = {

    deleteBucketObjectHandler: function deleteBucketObjectHandler() {
        var chain = [
            buckets.loadRequest,
            buckets.getBucketIfExists,
            auth.authorizationHandler(),
            buckets.maybeGetObject,
            conditional.conditionalRequest(),
            deleteObject,
            buckets.notFoundHandler,
            buckets.successHandler
        ];
        return (chain);
    }

};
