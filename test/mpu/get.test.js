/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var path = require('path');
var uuid = require('node-uuid');
var vasync = require('vasync');
var verror = require('verror');

if (require.cache[path.join(__dirname, '/../helper.js')])
    delete require.cache[path.join(__dirname, '/../helper.js')];
if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var testHelper = require('../helper.js');
var helper = require('./helper.js');

var after = testHelper.after;
var before = testHelper.before;
var test = testHelper.test;

var ifErr = helper.ifErr;

before(function (cb) {
    helper.initMPUTester.call(this, cb);
});


after(function (cb) {
    helper.cleanupMPUTester.call(this, cb);
});


// Get: bad input (happy path tested in create)

test('get upload: non-uuid id', function (t) {
    var self = this;
    var bogus = 'foobar';
    var action = 'state';
    var p = '/' + this.client.user + '/uploads/0/' + bogus + '/' + action;
    var options = {
        headers: {
            'content-type': 'application/json',
            'expect': 'application/json'
        }
    };

    self.client.signRequest({
        headers: options.headers
    },
    function (err) {
        if (ifErr(t, err, 'sign request')) {
            t.end();
            return;
        }

        self.client.get(p, options, function (err2, _, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }

            t.checkResponse(res, 404);
            t.ok(verror.hasCauseWithName(err2, 'ResourceNotFoundError'));
            t.end();
        });
    });
});


test('get upload: non-existent id', function (t) {
    var self = this;
    var bogus = uuid.v4();
    self.uploadId = bogus;

    self.getUpload(bogus, function (err, upload) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err, 'ResourceNotFoundError'), err);
        t.end();
    });
});
