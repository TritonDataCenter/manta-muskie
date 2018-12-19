/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var buckets = require('../buckets');
var common = require('../../common');
var obj = require('../../obj');


function putObject(req, res, next) {

    var bucketObject = req.bucketObject;
    var type = bucketObject.type;
    var log = req.log;
    log.debug({
        bucket_name: bucketObject.bucket_name,
        bucket_id: req.bucket_data.id,
        name: bucketObject.name
    }, 'putObject: requested');

    common.createMetadata(req, type, function onCreateMetadata(createErr, md) {
        if (createErr) {
            next(createErr);
            return;
        }

        log.debug({
            metadata: md
        }, 'putObjectMetadata: entered');

        /* libmanta's Moray helpers need this information to create metadata */
        var content_length = req.getContentLength() || 0;
        var content_type = req.headers['content-type'] || 'text/plain';
        var headers = {};
        var sharks = { 'us-east-1': '1.mako',
                       'us-east-2': '2.mako'
                     };
        var opts = {
            key: bucketObject.name,
            requestId: md.requestId,
            owner: md.owner,
            bucket_id: req.bucket_data.id,
            content_length: content_length,
            content_type: content_type,
            content_md5: obj.ZERO_BYTE_MD5,
            headers: headers,
            sharks: sharks,
            props: {},
            type: type,
            log: req.log
        };

        req.moray.putObject(opts, function onPutObject(createErr2, object_data) {
            if (createErr2) {
                log.debug(createErr2, 'putObject: failed');
                next(createErr2);
            } else {
                log.debug({
                    bucket: bucketObject.bucket_name,
                    object: bucketObject.name
                }, 'putObject: done');
                res.send(204, object_data);
                next(null, object_data);
            }
        });
    });
}


module.exports = {

    createBucketObjectHandler: function createBucketObjectHandler() {
        var chain = [
            // common.ensureBucketObjectHandler(),
            buckets.loadRequest,
            buckets.checkBucketExists,
            /*
             * Call into the storage object file to access pre-existing code
             * written to stream data to sharks.
             */
            // obj.putBucketObjectHandler()
            putObject
        ];
        return (chain);
    }

};
