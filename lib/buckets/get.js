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

function getBucket(req, res, next) {

    var bucket = req.bucket;
    var log = req.log;
    log.debug({
        name: bucket.name
    }, 'get-bucket: requested');

    /* libmanta's Moray helpers need this information to retrieve metadata */
    var opts = {
        key: req.key,
        requestId: req.getId()
    };

    common.loadMetadata(req, opts, function onLoadMetadata(err, md) {
        if (err) {
            log.debug(err, 'getBucketMetadata: failed');
            next(err);
            return;
        } else {
            bucket.mtime = new Date(md.mtime).toISOString();
            log.debug({
                bucket: bucket
            }, 'getBucketMetadata: done');
            res.send(200, bucket);
            next(null, md);
        }
    });

}

module.exports = {

    getBucketHandler: function getBucketHandler() {
        var chain = [
            buckets.loadRequest,
            getBucket
        ];
        return (chain);
    }

};
