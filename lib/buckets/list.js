/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var assert = require('assert-plus');

var auth = require('../auth');
var buckets = require('./buckets');
var common = require('./common');

function listBuckets(req, res, next) {

    var log = req.log;
    log.debug({
        key: req.key,
        params: req.params
    }, 'listBuckets: requested');

    var mreq = common.listBuckets(req);

    var entries = [];
    var message;

    mreq.once('error', function onError(err) {
        mreq.removeAllListeners('end');
        mreq.removeAllListeners('entry');

        log.debug(err, 'listBuckets: failed');
        next(err);
    });

    mreq.on('message', function onMessage(_message) {
        message = _message;
        assert.object(message, 'message');
        assert.bool(message.finished, message.finished);
    });

    mreq.on('entry', function onEntry(entry) {
        entries.push(entry);
    });

    mreq.once('end', function onEnd() {
        // ensure that we received a messaged
        assert.ok(message, 'message');

        log.debug({}, 'listBuckets: done');

        if (!message.finished) {
            // If we are not finished then we are certain there is at least 1
            // record received
            assert.ok(entries.length > 0, 'entries.length > 0');

            var lastEntry = entries[entries.length - 1];
            res.header('Next-Marker', lastEntry.name);
        }

        entries.forEach(function (entry) {
            res.write(JSON.stringify(entry, null, 0) + '\n');
        });

        res.end();
        next();
    });
}

function setAuthContext(req, res, next) {
    req.authContext.action = 'getdirectory';
    next();
}

module.exports = {

    listBucketsHandler: function listBucketsHandler() {
        var chain = [
            buckets.loadRequest,
            setAuthContext,
            auth.authorizationHandler(),
            listBuckets
        ];
        return (chain);
    }

};
