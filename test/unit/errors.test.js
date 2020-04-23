/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var test = require('@smaller/tap').test;
var verror = require('verror');

var errors = require('../../lib/errors');

test('ServiceUnavailableError', function (t) {
    var cause = new Error('cause');
    var err = new errors.ServiceUnavailableError(null, cause);

    t.ok(err instanceof errors.MuskieError, 'error is a MuskieError');

    t.equal(err.restCode, 'ServiceUnavailable',
        'err.restCode is "ServiceUnavailable", got ' + err.restCode);
    t.equal(err.statusCode, 503,
        'err.statusCode is 503, got ' + err.statusCode);
    t.equal(err.message, 'manta is unable to serve this request',
        'got expected err.message');

    // The upgrade to restify v6.x in MANTA-5148 has broken this. This will
    // be handled in a separate ticket: MANTA-5173.
    // t.deepEqual(err.cause(), cause);

    t.end();
});
