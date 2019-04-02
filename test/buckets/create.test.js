/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var testHelper = require('../helper.js');
var helper = require('./helper.js');
var ifErr = helper.ifErr;

var after = testHelper.after;
var before = testHelper.before;
var test = testHelper.test;


before(function (cb) {
    helper.initBucketTester.call(this, cb);
});

after(function (cb) {
    helper.cleanupBucketTester.call(this, cb);
});

test('run a test', function (t) {
    var self = this;

    t.ok(self, 'self');
    t.end();
});

test('create bucket', function (t) {
    var self = this;
    var h = {
        'content-type': 'application/json; type=bucket'
    };
    var p = self.bucketsRoot + '/newtestbucket';

    self.createBucket(p, h, function (err, res) {
        if (ifErr(t, err, 'create bucket')) {
            t.end();
            return;
        }
        t.ok(res, 'res');
        t.checkResponse(res, 204);
        t.end();
    });
});
