/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var util = require('util');

var buckets = require('./buckets');
var errors = require('../errors');

var sprintf = util.format;

function deleteBucket(req, res, next) {

    var bucket = req.bucket;
    var log = req.log;
    log.debug({
        name: bucket.name
    }, 'deleteBucket: requested');

    /* libmanta's Moray helpers need this information to retrieve metadata */
    var opts = {
        key: bucket.name,
        requestId: req.getId(),
        owner: req.owner.account.uuid,
        type: 'bucket'
    };

    req.moray.deleteBucket(opts, function onDeleteMetadata(err, md) {
        if (err) {
            log.debug(err, 'deleteBucketMetadata: failed');
            next(err);
            return;
        } else if (md === 0) {
            var muskieErr = errors.MuskieError.call(this, {
                restCode: 'BucketNotFound',
                statusCode: 404,
                message: sprintf('%s was not found', bucket.name)
            });
            log.debug({
                error: muskieErr
            }, 'deleteBucket: not found');
            next(muskieErr);
            return;
        } else {
            log.debug({
                bucket: bucket
            }, 'deleteBucketMetadata: done');
            res.send(204, null);
            next();
        }
    });

}

module.exports = {

    deleteBucketHandler: function deleteBucketHandler() {
        var chain = [
            buckets.loadRequest,
            deleteBucket
        ];
        return (chain);
    }

};
