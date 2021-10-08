/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var _helper = __dirname + '/helper.js';
if (require.cache[_helper])
    delete require.cache[_helper];
var helper = require(_helper);

var util = require('util');
var verror = require('verror');

var errors = require('../lib/errors');

var test = helper.test;

test('ServiceUnavailableError', function (t) {
    var cause = new Error('cause');
    var err = new errors.ServiceUnavailableError(null, cause);

    t.ok(err instanceof errors.MuskieError, 'error is a MuskieError');
    t.equal(err.restCode, 'ServiceUnavailable');
    t.equal(err.statusCode, 503);
    t.equal(err.message, 'manta is unable to serve this request');
    t.deepEqual(err.cause(), cause);

    t.end();
});
