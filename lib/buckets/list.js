/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var common = require('../common');

function listBuckets(req, res, next) {

    var log = req.log;
    log.debug({
        bucket: req.key
    }, 'listBuckets: requested');

    var obj = {
        bucket_data: req.bucket_data
    };

    /* This function uses an emitter rather than callback pattern for speed. */
    var mreq = common.bucketList(req.key, req, obj);

    mreq.once('error', function onError(err) {
        mreq.removeAllListeners('end');
        mreq.removeAllListeners('entry');

        log.debug(err, 'listBuckets: failed');
        next(err);
    });

    mreq.on('entry', function onEntry(bucket) {
        res.write(JSON.stringify(bucket, null, 0) + '\n');
        next();
    });

    mreq.once('end', function onEnd() {
        log.debug({}, 'listBuckets: done');
        res.end();
        next();
    });

}

module.exports = {

    listBucketsHandler: function listBucketsHandler() {
        var chain = [
            listBuckets
        ];
        return (chain);
    }

};
