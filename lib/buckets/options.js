/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var auth = require('../auth');
var buckets = require('./buckets');
var common = require('./common');

function options(req, res, next) {

    var log = req.log;
    log.debug('options: requested');

    res.setHeader('Allow', 'OPTIONS, GET');
    res.send(204);

    log.debug('options: done');

    next();
}

module.exports = {

    optionsBucketsHandler: function optionsBucketsHandler() {
        var chain = [
            buckets.loadRequest,
            auth.authorizationHandler(),
            options
        ];
        return (chain);
    }

};
