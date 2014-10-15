/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var libmanta = require('libmanta');

var auth = require('../auth');
var jobsCommon = require('./common');



///--- API

function getJob(req, res, next) {
    var j = libmanta.translateJob(req.job);

    res.send(200, j);
    next();
}



///--- Exports


module.exports = {

    getHandler: function getHandler() {
        var chain = [
            jobsCommon.loadJob,
            jobsCommon.jobContext,
            auth.authorizationHandler(),
            getJob
        ];
        return (chain);
    }
};
