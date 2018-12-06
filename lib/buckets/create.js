/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var buckets = require('./buckets');
var common = require('../common');

function createBucket(req, res, next) {

    var bucket = req.bucket;
    var type = bucket.type;
    var log = req.log;
    log.debug({
        name: bucket.name
    }, 'createBucket: requested');

    common.createMetadata(req, type, function onCreateMetadata(createErr, md) {
        if (createErr) {
            next(createErr);
            return;
        }

        log.debug({
            metadata: md
        }, 'createBucketMetadata: entered');

        /* libmanta's Moray helpers need this information to create metadata */
        var opts = {
            key: md.key,
            requestId: md.requestId,
            owner: md.owner,
            type: type
        };

        req.moray.putMetadata(opts, function onPutMetadata(putErr) {
            if (putErr) {
                log.debug(putErr, 'createBucketMetadata: failed');
                next(putErr);
            } else {
                log.debug({
                    bucket: bucket
                }, 'createBucketMetadata: done');
                res.send(204, bucket);
                next(null, md);
            }
        });
    });


}

module.exports = {

    createBucketHandler: function createBucketHandler() {
        var chain = [
            common.ensureBucketRootHandler(),
            buckets.loadRequest,
            createBucket
        ];
        return (chain);
    }

};
