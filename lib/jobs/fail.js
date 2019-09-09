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


///--- API


function getJobFailures(req, res, next) {
    req.log.debug({id: req.job.id}, 'getJobFailures: entered');

    var keys = [];
    var log = req.log;
    var opts = {
        limit: 100,
        log: log,
        sort_order: 'DESC'
    };
    var query = req.marlin.jobFetchFailedJobInputs(req.params.id, opts);

    query.once('error', function (err) {
        query.removeAllListeners('end');
        query.removeAllListeners('key');
        next(err);
    });

    query.on('key', function (key) {
        keys.push(key);
    });

    query.once('end', function () {
        query.removeAllListeners('error');
        query.removeAllListeners('key');

        keys.reverse();

        res.header('Content-Type', 'text/plain');
        res.writeHead(200);
        keys.forEach(function (k) {
            res.write(k + '\n');
        });
        res.end();

        next();
    });
}




///--- Exports

module.exports = {
    getFailuresHandler: function getFailuresHandler() {
        var chain = [
            jobsCommon.loadJob,
            jobsCommon.jobContext,
            auth.authorizationHandler(),
            getJobFailures
        ];

        return (chain);
    }
};
