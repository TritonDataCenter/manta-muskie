/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var auth = require('../auth');
var jobsCommon = require('./common');



///--- API

function setup(req, res, next) {
    req.log = req.log.child({jobId: req.job.jobId}, true);
    req.marlin_opts = {
        log: req.log,
        retry: {
            retries: 2
        }
    };
    next();
}

function endJobInput(req, res, next) {
    if (req.job.timeInputDone) {
        req.log.debug('endJobInput: already done');
        next();
        return;
    }

    req.log.debug('endJobInput: entered');

    req.marlin.jobEndInput(req.job.jobId, req.marlin_opts, next);
}


function cancelJob(req, res, next) {
    req.log.debug('cancelJob: entered');
    req.marlin.jobCancel(req.job.jobId, req.marlin_opts, next);
}


function respond(req, res, next) {
    res.send(202);
    next();
}



///--- Exports


module.exports = {
    endInputHandler: function endInputHandler() {
        var chain = [
            jobsCommon.loadJob,
            jobsCommon.jobContext,
            auth.authorizationHandler(),
            jobsCommon.ensureJobState,
            setup,
            endJobInput,
            respond
        ];
        return (chain);
    },

    cancelHandler: function cancelHandler() {
        var chain = [
            jobsCommon.loadJob,
            jobsCommon.jobContext,
            auth.authorizationHandler(),
            jobsCommon.ensureJobState,
            setup,
            cancelJob,
            respond
        ];
        return (chain);
    }
};
