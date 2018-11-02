/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');

function loadRequest(req, res, next) {
    req.bucket = new Bucket(req);
    next();
}

function Bucket(req) {

    var self = this;

    assert.object(req, 'req');
    if (req.params.bucket_name) {
        self.name = req.params.bucket_name;
    }
    self.type = 'bucket';

    return (self);

}

module.exports = {
    Bucket: Bucket,
    loadRequest: loadRequest
};
