/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var auth = require('../../auth');
var buckets = require('../buckets');
var common = require('../../common');
var conditional = require('../../conditional_request');
var errors = require('../../errors');
var obj = require('../../obj');


module.exports = {

    getBucketObjectHandler: function getBucketObjectHandler() {
        var chain = [
            buckets.loadRequest,
            buckets.getBucketIfExists,
            buckets.getObject,
            auth.authorizationHandler(),
            conditional.conditionalRequest(),
            buckets.notFoundHandler,
            common.streamFromSharks
        ];
        return (chain);
    }
};
