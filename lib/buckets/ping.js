/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var buckets = require('./buckets');
var common = require('../common');

function pingBucket(req, res, next) {

    var bucket = req.bucket;
    var log = req.log;
    log.debug({
        name: bucket.name
    }, 'pingBuckets: requested');

    /* libmanta's Moray helpers need this information to retrieve metadata */
    var opts = {
        key: req.key,
        requestId: req.getId()
    };

    common.loadMetadata(req, opts, function onLoadMetadata(err, md) {
        if (err) {
            log.debug(err, 'pingBucketMetadata: failed');
            next(err);
            return;
        } else {
            log.debug('pingBucketMetadata: done');
            res.send(200);
            next(null, md);
        }
    });

}

module.exports = {

    pingBucketHandler: function pingBucketHandler() {
        var chain = [
            buckets.loadRequest,
            pingBucket
        ];
        return (chain);
    }

};
