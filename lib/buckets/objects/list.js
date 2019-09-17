/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var assert = require('assert-plus');

var auth = require('../../auth');
var buckets = require('../buckets');
var common = require('../common');

function listBucketObjects(req, res, next) {
    var bucket = req.bucket;
    var log = req.log;

    assert.uuid(bucket.id, 'bucket.id');

    log.debug({
        bucket: req.name,
        params: req.params
    }, 'listBucketObjects: requested');

    var mreq = common.listObjects(req, bucket.id);

    var entries = [];
    var message;

    mreq.once('error', function onError(err) {
        mreq.removeAllListeners('end');
        mreq.removeAllListeners('entry');

        log.debug(err, 'listBucketObjects: failed');
        next(err);
    });

    mreq.on('message', function onMessage(_message) {
        message = _message;
        assert.object(message, 'message');
        assert.bool(message.finished, message.finished);
    });

    mreq.on('entry', function onEntry(entry, raw) {
        entries.push({
            entry: entry,
            raw: raw
        });
    });

    mreq.once('end', function onEnd() {
        // ensure that we received a messaged
        assert.ok(message, 'message');

        log.debug({}, 'listBucketObjects: done');

        if (!message.finished) {
            // If we are not finished then we are certain there is at least 1
            // record received
            assert.ok(entries.length > 0, 'entries.length > 0');

            var lastObject = entries[entries.length - 1];
            var lastEntry = lastObject.entry;
            var lastRaw = lastObject.raw;

            assert.object(lastEntry, 'lastEntry');
            assert.string(lastEntry.name, 'lastEntry.name');

            assert.object(lastRaw, 'lastRaw');
            assert.optionalString(lastRaw.nextMarker, 'lastRaw.nextMarker');

            res.header('Next-Marker', lastRaw.nextMarker || lastEntry.name);
        }

        entries.forEach(function (obj) {
            var entry = obj.entry;
            res.write(JSON.stringify(entry, null, 0) + '\n');
        });

        res.end();
        next();
    });
}


module.exports = {

    listBucketObjectsHandler: function listBucketObjectsHandler() {
        var chain = [
            buckets.loadRequest,
            auth.authorizationHandler(),
            buckets.getBucketIfExists,
            listBucketObjects
        ];
        return (chain);
    }

};
