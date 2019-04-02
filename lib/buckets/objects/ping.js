/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var buckets = require('../buckets');
var common = require('../../common');
var obj = require('../../obj');

function pingBucketObject(req, res, next) {

    var bucketObject = req.bucketObject;
    var log = req.log;
    log.debug({
        bucket_name: bucketObject.bucket_name,
        name: bucketObject.name
    }, 'pingBucketObject: requested');

    /* libmanta's Moray helpers need this information to retrieve metadata */
    var opts = {
        key: req.key,
        requestId: req.getId()
    };

    common.loadMetadata(req, opts,
      function pingBucketObjectMetadata(loadErr, md) {

        if (loadErr) {
            log.debug({
                key: opts.key,
                metadata: md.value,
                requestId: md.requestId
            }, 'pingBucketObjectMetadata: failed');
            next(loadErr);
            return;
        }

        if (!md || md.type === null) {
            var mdErr = new ResourceNotFoundError(opts.key);
            log.debug({
                error: mdErr
            }, 'pingBucketObjectMetadata: not found');
            next(mdErr);
            return;
        }

        req.metadata = md;
        req.metadata.type = bucketObject.type;
        req.metadata.sharks = md.sharks;

        log.debug({
            key: opts.key,
            requestId: opts.requestId,
            metadata: req.metadata
        }, 'pingBucketObjectMetadata: done');
        next(null, md);

    });

}

function onlySendHead(req, res, err, next) {

    var log = req.log;

    if (err) {
        log.debug(err, 'pingBucketObject: failed');
        res.send(404);
        next(err);
        return;
    } else {
        log.debug({}, 'pingBucketObject: done');
        res.send(200);
        next(null);
    }

}


module.exports = {

    pingBucketObjectHandler: function pingBucketObjectHandler() {
        var chain = [
            buckets.loadRequest,
            buckets.checkBucketExists,
            pingBucketObject,
            /*
             * Call into the storage object file to access pre-existinge code
             * written to stream data from shards.
             */
            obj.getBucketObjectHandler(),
            onlySendHead
        ];
        return (chain);
    }

};
