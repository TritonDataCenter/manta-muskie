/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var buckets = require('../buckets');
var common = require('../../common');
var conditional = require('../../conditional_request');
var errors = require('../../errors');
var obj = require('../../obj');

function headObject(req, res, next) {
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
    }, 'headBucketObject: requested');


    var onGetObject = function onGet(err, object_data) {
        if (err) {
            log.debug({
                err: err,
                owner: owner,
                bucket: bucket.name,
                bucket_id: bucket.id,
                object: bucketObject.name
            }, 'headObject: error reading object metadata');

            next(err);
            return;
        } else {
            // There is only one error type returned by this RPC
            if (object_data.error && object_data.error.name ===
               'ObjectNotFound') {
                    req.resource_exists = false;
                    req.not_found_error =
                        new errors.ObjectNotFoundError(bucketObject.name);
            } else {
                log.debug({
                    owner: owner,
                    bucket: bucket.name,
                    bucket_id: bucket.id,
                    object: bucketObject.name
                }, 'headObject: done');

                req.resource_exists = true;
                req.metadata = req.metadta || {};
                req.metadata.type = 'bucketobject';
                req.metadata.objectId = object_data.id;
                req.metadata.contentMD5 = object_data.content_md5;
                req.metadata.contentLength = object_data.content_length;
                req.metadata.contentType = object_data.content_type;

                // Add other needed response headers
                res.set('Etag', object_data.id);
                res.set('Last-Modified', new Date(object_data.modified));
                res.set('Durability-Level', object_data.sharks.length);
                res.set('Content-Length', object_data.content_length);
                res.set('Content-MD5', object_data.content_md5);
                res.set('Content-Type', object_data.content_type);

                Object.keys(object_data.headers).forEach(function (k) {
                    if (/^m-\w+/.test(k)) {
                        res.set(k, object_data.headers[k]);
                    }
                });
            }

            next();
        }
    };

    req.boray.client.getObjectNoVnode(owner, bucket.id, bucketObject.name,
        requestId, onGetObject);
}

module.exports = {

    headBucketObjectHandler: function headBucketObjectHandler() {
        var chain = [
            buckets.loadRequest,
            buckets.getBucketIfExists,
            headObject,
            conditional.conditionalRequest(),
            buckets.notFoundHandler,
            buckets.successHandler
        ];
        return (chain);
    }

};
