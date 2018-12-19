/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var buckets = require('../buckets');

function deleteBucketObject(req, res, next) {

    var bucketObject = req.bucketObject;
    var log = req.log;
    log.debug({
        bucket_name: bucketObject.bucket_name,
        name: bucketObject.name
    }, 'deleteBucketObject: requested');

    /* libmanta's Moray helpers need this information to retrieve metadata */
    var opts = {
        key: bucketObject.name,
        owner: req.owner.account.uuid,
        bucket_id: req.bucket_data.id,
        requestId: req.getId()
    };

    req.moray.delObject(opts, function onDeleteMetadata(err, md) {
        if (err) {
            log.debug(err, 'deleteBucketMetadata: failed');
            next(err);
            return;
        } else {
            log.debug({
                bucketObject: bucketObject
            }, 'deleteBucketMetadata: done');
            res.send(204, null);
            next();
        }
    });

}

module.exports = {

    deleteBucketObjectHandler: function deleteBucketObjectHandler() {
        var chain = [
            buckets.loadRequest,
            buckets.checkBucketExists,
            deleteBucketObject
        ];
        return (chain);
    }

};
