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

function getJobOutput(req, res, next) {
    req.log.debug({id: req.job.id}, 'getJobOutput: entered');

    var keys = [];
    var log = req.log;
    var opts = {
        limit: 100,
        log: log,
        sort_order: 'DESC'
    };
    var pi = req.job.phases.length - 1;
    var query = req.marlin.jobFetchOutputs(req.params.id, pi, opts);

    query.on('key', function (k) {
        keys.push(k);
    });

    query.once('error', function (err) {
        query.removeAllListeners('end');
        query.removeAllListeners('key');

        next(err);
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

        log.debug('getJobOutput: done');
        next();
    });
}



///--- Exports

module.exports = {
    getOutputHandler: function getOutputHandler() {
        var chain = [
            jobsCommon.jobContext,
            auth.authorizationHandler(),
            jobsCommon.loadJob,
            getJobOutput
        ];

        return (chain);
    }
};
