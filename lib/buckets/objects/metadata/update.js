/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var assert = require('assert-plus');
var auth = require('../../../auth');
var buckets = require('../../buckets');
var common = require('../../../common');
var conditional = require('../../../conditional_request');
var libuuid = require('libuuid');
var obj = require('../../../obj');


function updateObjectMetadata(req, res, next) {
    var owner = req.owner.account.uuid;
    var bucket = req.bucket;
    var bucketObject = req.bucketObject;
    var objectId = libuuid.create();
    var type = bucket.type;
    var props = {};
    var log = req.log;

    log.debug({
        owner: owner,
        bucket_name: bucket.name,
        bucket_id: bucket.id,
        object: bucketObject.name
    }, 'createObject: requested');

    buckets.createObjectMetadata(req, type,
        function onCreateMetadata(createErr, object_data) {

        if (createErr) {
            next(createErr);
            return;
        }

        log.debug({
            owner: owner,
            bucket_name: bucket.name,
            bucket_id: bucket.id,
            object: bucketObject.name,
            metadata: object_data
        }, 'onCreateMetadata: entered');

        var onUpdateObject = function onUpdate(createErr2, response_data) {
            if (response_data !== undefined && response_data._node) {
                // Record the name of the shard and vnode contacted.
                req.entryShard = response_data._node.pnode;
                req.entryVnode = response_data._node.vnode;
            }

            if (createErr2) {
                log.debug(createErr2, 'createObject: failed');
                next(createErr2);
            } else {
                log.debug({
                    bucket: bucketObject.bucket_name,
                    object: bucketObject.name
                }, 'createObject: done');
                if (req.headers['origin']) {
                    res.header('Access-Control-Allow-Origin',
                               req.headers['origin']);
                }
                res.header('Etag', response_data.id);
                res.header('Last-Modified', new Date(response_data.modified));

                Object.keys(response_data.headers).forEach(function (k) {
                    if (/^m-\w+/.test(k)) {
                        res.set(k, response_data.headers[k]);
                    }
                });

                res.send(204);
                next(null, response_data);
            }
        };

        req.boray.client.updateObjectNoVnode(owner, bucket.id,
            bucketObject.name, objectId, object_data.contentType,
            object_data.headers, props, onUpdateObject);
    });
}


function parseArguments(req, res, next)  {
    if ([
        'content-length',
        'content-md5',
        'durability-level'
    ].some(function (k) {
        var bad = req.headers[k];
        if (bad) {
            setImmediate(function killRequest() {
                next(new InvalidUpdateError(k));
            });
        }
        return (bad);
    })) {
        return;
    }

    req.log.debug('updateObjectMetadata:parseArguments: done');
    next();
}


module.exports = {
    updateBucketObjectMetadataHandler:
        function updateBucketObjectMetadataHandler() {
        var chain = [
            buckets.loadRequest,
            buckets.getBucketIfExists,
            auth.authorizationHandler(),
            buckets.maybeGetObject,
            conditional.conditionalRequest(),
            parseArguments,  // not blocking
            updateObjectMetadata // blocking
        ];
        return (chain);
    }
};
