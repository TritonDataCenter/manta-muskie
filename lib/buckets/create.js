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
            key: req.params.bucket_name,
            requestId: md.requestId,
            owner: md.owner,
            type: type,
            log: req.log
        };

        req.moray.createBucket(opts, function onCreateBucket(createErr2, bucket_data) {
            if (createErr2) {
                log.debug(createErr2, 'createBucket: failed');
                next(createErr2);
            } else {
                log.debug({
                    bucket: bucket
                }, 'createBucket: done');
                res.send(204, bucket_data);
                next(null, bucket_data);
            }
        });
    });


}

module.exports = {

    createBucketHandler: function createBucketHandler() {
        var chain = [
            // common.ensureBucketRootHandler(),
            buckets.loadRequest,
            createBucket
        ];
        return (chain);
    }

};
