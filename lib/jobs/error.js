/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var auth = require('../auth');
var jobsCommon = require('./common');



///--- Globals

var JOB_ERR_CONTENT_TYPE = 'application/x-json-stream; type=job-error';



///--- API

function getJobErrors(req, res, next) {
    req.log.debug({id: req.job.id}, 'getJobErrors: entered');

    var errs = [];
    var log = req.log;
    var opts = {
        limit: 100,
        log: log,
        sort_order: 'DESC'
    };
    var query = req.marlin.jobFetchErrors(req.params.id, opts);

    query.once('error', function (err) {
        query.removeAllListeners('end');
        query.removeAllListeners('err');
        next(err);
    });

    query.on('err', function (err) {
        errs.push({
            phase: err.phaseNum,
            what: err.what,
            code: err.code,
            message: err.message,
            stderr: err.stderr,
            core: err.core,
            input: err.input || err.key,
            p0input: err.p0input || err.p0key
        });
    });

    query.once('end', function () {
        query.removeAllListeners('err');
        query.removeAllListeners('error');

        errs.reverse();

        res.header('Content-Type', JOB_ERR_CONTENT_TYPE);
        res.writeHead(200);
        errs.forEach(function (e) {
            res.write(JSON.stringify(e, null, 0) + '\n');
        });
        res.end();

        next();
    });
}



///--- Exports

module.exports = {
    getErrorsHandler: function getErrorsHandler() {
        var chain = [
            jobsCommon.loadJob,
            jobsCommon.jobContext,
            auth.authorizationHandler(),
            getJobErrors
        ];

        return (chain);
    }
};
