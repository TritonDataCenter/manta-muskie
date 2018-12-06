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

function listBucketObjects(req, res, next) {

    var bucket = req.bucket;
    var log = req.log;
    log.debug({
        bucket: bucket.name
    }, 'listBucketObjects: requested');

    /* This function uses an emitter rather than callback pattern for speed. */
    var mreq = common.readdir(req.key, req);

    mreq.once('error', function onError(err) {
        mreq.removeAllListeners('end');
        mreq.removeAllListeners('entry');

        log.debug(err, 'listBucketObjects: failed');
        next(err);
    });

    mreq.on('entry', function onEntry(bucketObject) {
        res.write(JSON.stringify(bucketObject, null, 0) + '\n');
        next();
    });

    mreq.once('end', function onEnd() {
        log.debug({}, 'listBucketObjects: done');
        res.end();
        next();
    });

}

module.exports = {

    listBucketObjectsHandler: function listBucketObjectsHandler() {
        var chain = [
            buckets.loadRequest,
            buckets.checkBucketExists,
            listBucketObjects
        ];
        return (chain);
    }

};
