/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert-plus');
var common = require('../common.js');

require('../errors');



///--- API

/*
 * For existing jobs, treat the job's directory (/jobs/<jobid>) as the resource.
 */
function jobContext(req, res, next) {
    var opts = {
        // keys like /<accountuuid>/jobs/<jobid>/live/<status|cancel|..>
        key: req.key.split('/').slice(0, 4).join('/'),
        requestId: req.getId()
    };

    common.loadMetadata(req, opts, function (err, md) {
        if (err) {
            next(err);
            return;
        }

        req.authContext.resource = {
            owner: req.owner,
            key: md.key || req.key,
            roles: md.roles || []
        };

        next();
    });
}


/*
 * For listing and job creation, use the root /jobs directory as the resource.
 */
function jobContextRoot(req, res, next) {
    var opts = {
        key: req.key.split('/').slice(0, 3).join('/'),
        requestId: req.getId()
    };

    common.loadMetadata(req, opts, function (err, md) {
        if (err) {
            next(err);
            return;
        }

        req.authContext.resource = {
            owner: req.owner,
            key: md.key || req.key,
            roles: md.roles || []
        };

        next();
    });
}

function loadJob(req, res, next) {
    var id = req.params.id;
    var job;
    var key = req.path();
    var log = req.log;

    log.debug({id: id}, 'loadJob: entered');

    /*
     * The only requests that set useJobCache today are requests to add job
     * inputs.  For these requests, we want to fetch the job from the source if
     * it doesn't have its domain (supervisor) set.
     */
    if (req.useJobCache && (job = req.jobCache.get(key)) && job.worker) {
        req.job = job;
        next();
        return;
    }

    req.marlin.jobFetch(id, {log: log}, function (err, record) {
        if (err) {
            log.debug(err, 'loadJob: failed to get job');
            next(err);
        } else {
            req.job = record.value;
            req.jobCache.set(key, record.value);
            log.debug({job: req.job}, 'loadJob: done');
            next();
        }
    });
}


function limit(req) {
    var l = parseInt(req.params.limit || 1000, 10);

    return (Math.abs(Math.min(l, 1000)));
}


function ensureJobState(req, res, next) {
    assert.ok(req.job);

    var id = req.params.id;
    var job = req.job;

    if (job.state === 'done') {
        next(new JobStateError(id, 'done'));
    } else if (job.timeCancelled) {
        next(new JobStateError(id, 'cancelled'));
    } else if (req.ensureInputNotDone && job.timeInputDone) {
        next(new JobStateError(id, 'inputDone'));
    } else {
        next();
    }
}



///--- Exports

module.exports = {
    loadJob: loadJob,
    limit: limit,
    jobContext: jobContext,
    jobContextRoot: jobContextRoot,
    ensureJobState: ensureJobState
};
