/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var assert = require('assert-plus');
var libmanta = require('libmanta');
var once = require('once');

var auth = require('../auth');
var common = require('../common');
var errors = require('../errors');
var jobsCommon = require('./common');
require('../errors');



///--- Globals

var translateJob = libmanta.translateJob;

var JOB_CONTENT_TYPE = 'application/x-json-stream; type=job';
var VALID_STATES = ['live', 'running'];
var NAME_REGEX = /^[\w\d-]+$/;



///--- Helpers

// This is always to be wrapped with `once`:
// slightly subtle, in that if it's the first call, we'll always return true,
// but any other calls return undefined.  So if it's an error case, we
// return true, and _don't_ write the header, so callers can just call next(err)
// however otherwise we write 200 and the content-type
function _writeHead(res, err) {
    if (!err) {
        res.setHeader('Content-Type', JOB_CONTENT_TYPE);
        res.writeHead(200);
    }

    return (true);
}



///--- APIs

function listLiveJobs(req, res, next) {
    var log = req.log;

    //Some query error checking
    if (req.params.state &&
        VALID_STATES.indexOf(req.params.state) === -1) {
        next(new errors.InvalidParameterError(
            'state', req.params.state));
        return;
    }
    if (req.params.status &&
        VALID_STATES.indexOf(req.params.status) === -1) {
        next(new errors.InvalidParameterError(
            'status', req.params.status));
        return;
    }
    if (req.params.name &&
        !NAME_REGEX.test(req.params.name)) {
        next(new errors.InvalidParameterError(
            'name', req.params.name));
        return;
    }

    var query = req.marlin.jobsList({
        owner: req.owner.account.uuid,
        state: req.params.state,
        name: req.params.name,
        limit: jobsCommon.limit(req),
        log: req.log
    });
    var writeHead = once(_writeHead.bind(null, res));

    query.once('error', function (err) {
        log.debug(err, 'listLiveJobs: failed');

        query.removeAllListeners('end');
        query.removeAllListeners('record');

        if (writeHead(err)) {
            next(err);
        } else {
            res.end();
            next(false);
        }
    });

    query.on('record', function (record) {
        log.debug({record: record}, 'listLiveJobs: record found');
        var job = {
            name: record.value.jobId,
            type: 'directory',
            mtime: record.value.timeCreated
        };
        writeHead();
        res.write(JSON.stringify(job, null, 0) + '\n');
    });

    query.once('end', function () {
        log.debug('listLiveJobs: done');
        res.end();
        next();
    });
}



///--- Exports

module.exports = {
    listHandler: function listHandler() {
        var chain = [
            jobsCommon.jobContextRoot,
            auth.authorizationHandler(),
            listLiveJobs
        ];
        return (chain);
    }
};
