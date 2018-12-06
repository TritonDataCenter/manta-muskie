/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var buckets = require('../buckets');
var common = require('../../common');
var obj = require('../../obj');

module.exports = {

    createBucketObjectHandler: function createBucketObjectHandler() {
        var chain = [
            common.ensureBucketObjectHandler(),
            buckets.loadRequest,
            buckets.checkBucketExists,
            /*
             * Call into the storage object file to access pre-existing code
             * written to stream data to sharks.
             */
            obj.putBucketObjectHandler()
        ];
        return (chain);
    }

};
