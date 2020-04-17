/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var path = require('path');
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


var mpuEnabled = Boolean(require('../../etc/config.json').enableMPU);
if (mpuEnabled) {

before(function (cb) {
    helper.initMPUTester.call(this, cb);
});


after(function (cb) {
    helper.cleanupMPUTester.call(this, cb);
});


// Delete parts/upload directories: allowed cases

test('del upload directory with operator override', function (t) {
    var self = this;

    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            query: {
                allowMpuDeletes: true
            }
        };
        self.operatorClient.unlink(self.uploadPath(), opts,
        function (err2, res) {
            t.ok(err2, 'should fail to unlink upload dir even with operator ' +
                'override');

            if (!err2) {
                t.end();
                return;
            }

            t.ok(verror.hasCauseWithName(err2,
                'MethodNotAllowedError'),
                'should not be allowed to delete upload directory.');
            t.checkResponse(res, 405);

            t.end();
        });
    });
});


test('del part with operator override', function (t) {
    var self = this;

    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.MIN_PART_NUM;
        self.writeTestObject(self.uploadId, pn, function (err2, _) {
            if (ifErr(t, err, 'uploaded part')) {
                t.end();
                return;
            }

            var opts = {
                query: {
                    allowMpuDeletes: true
                }
            };
            self.operatorClient.unlink(self.uploadPath(pn), opts,
            function (err3, res) {
                t.ok(err3, 'should fail to unlink upload part even with ' +
                    'operator override');

                if (!err3) {
                    t.end();
                    return;
                }

                t.ok(verror.hasCauseWithName(err3,
                    'MethodNotAllowedError'),
                    'should not be allowed to delete upload part.');
                t.checkResponse(res, 405);
                t.end();
            });
        });
    });
});

// Delete parts/upload directories: non-operator, no override

test('del upload directory: non-operator without override', function (t) {
    var self = this;
    var h = {};

    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {};

        self.client.unlink(self.uploadPath(), opts, function (err2, res) {
            t.ok(err2, 'should not be able to delete upload dir');
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2,
                'MethodNotAllowedError'),
                'should not be allowed to delete upload directory.');
            t.checkResponse(res, 405);
            t.end();
        });
    });
});


test('del part: non-operator without override', function (t) {
    var self = this;
    var h = {};

    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.MIN_PART_NUM;
        self.writeTestObject(self.uploadId, pn, function (err2, _) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            var opts = {};

            self.client.unlink(self.uploadPath(pn), opts, function (err3, res) {
                t.ok(err3, 'should not be able to delete upload part');
                if (!err3) {
                    return (t.end());
                }
                t.ok(verror.hasCauseWithName(err3,
                    'MethodNotAllowedError'), err3);
                t.checkResponse(res, 405);
                t.end();
            });
        });
    });
});

} // mpuEnabled
