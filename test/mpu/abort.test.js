/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var uuid = require('node-uuid');
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
var computePartsMD5 = helper.computePartsMD5;
var createPartOptions = helper.createPartOptions;
var writeObject = helper.writeObject;


before(function (cb) {
    helper.initMPUTester.call(this, cb);
});


after(function (cb) {
    helper.cleanupMPUTester.call(this, cb);
});


test('abort upload', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.abortUpload(self.uploadId, function (err2) {
            if (ifErr(t, err2, 'aborted upload')) {
                t.end();
                return;
            }

            self.getUpload(self.uploadId, function (err3, upload) {
                if (ifErr(t, err3, 'got upload')) {
                    t.end();
                    return;
                }

                t.deepEqual(upload.headers, {});
                t.equal(upload.state, 'done');
                t.equal(upload.result, 'aborted');
                t.end();
            });
        });
    });
});


test('abort upload: upload already aborted', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.abortUpload(self.uploadId, function (err2) {
            if (ifErr(t, err2, 'aborted upload')) {
                t.end();
                return;
            }

            self.abortUpload(self.uploadId, function (err3) {
                if (ifErr(t, err3, 'aborted upload')) {
                    t.end();
                    return;
                }

                self.getUpload(self.uploadId, function (err4, upload) {
                    if (ifErr(t, err4, 'got upload')) {
                        t.end();
                        return;
                    }

                    t.deepEqual(upload.headers, {});
                    t.equal(upload.state, 'done');
                    t.equal(upload.result, 'aborted');
                    t.end();
                });
            });
        });
    });
});


// Abort: bad input

test('abort upload: upload already committed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.commitUpload(self.uploadId, [], function (err2) {
            if (ifErr(t, err2, 'commited upload')) {
                t.end();
                return;
            }

            self.abortUpload(self.uploadId, function (err3) {
                t.ok(err3);
                if (!err3) {
                    t.end();
                    return;
                }
                t.ok(verror.hasCauseWithName(err3,
                    'InvalidMultipartUploadStateError'), err3);
                t.end();
            });
        });
    });
});


test('abort upload: non-uuid id', function (t) {
    var self = this;
    var bogus = 'foobar';
    var action = 'abort';

    var options = {
        headers: {
            'content-type': 'application/json',
            'expect': 'application/json'
        },
        path: '/' + this.client.user + '/uploads/0/' + bogus + '/' + action
    };

    self.client.signRequest({
        headers: options.headers
    },
    function (err) {
        if (ifErr(t, err, 'sign request')) {
            t.end();
            return;
        }

        // We use the jsonClient directly, or we will blow a non-uuid assert
        // in the Manta client.
        self.client.jsonClient.post(options, {}, function (err2, _, res) {
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


test('abort upload: non-existent id', function (t) {
    var self = this;
    var bogus = uuid.v4();

    self.getUpload(bogus, function (err, upload) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err, 'ResourceNotFoundError'), err);
        t.end();
    });
});
