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

test('list buckets', function (t) {
    var self = this;
    var h = {
        'content-type': 'application/json; type=bucket'
    };
    var p = self.bucketsRoot + '/listtestbucket';

    self.createBucket(p, h, function (err, res) {
        if (ifErr(t, err, 'create bucket')) {
            t.ok(res, 'res');
            t.end();
            return;
        }
        self.client.ls(self.bucketsRoot, function (err2, res2) {

            if (err2) {
                t.end();
                return;
            }

            res2.once('error', function (err3) {
                ifErr(t, err3, 'retrieve buckets');
                t.end();
            });

            res2.once('end', function (res3) {
                t.ok(res3);
                t.checkResponse(res3, 200);
                t.end();
            });

            t.ok(res2, 'res');
            t.checkResponse(res2, 200);
            t.end();
        });
    });
});
