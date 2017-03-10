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
var checkCreateResponse = helper.checkCreateResponse;
var computePartsMD5 = helper.computePartsMD5;
var createPartOptions = helper.createPartOptions;
var createUpload = helper.createUpload;
var sanityCheckUpload = helper.sanityCheckUpload;
var writeObject = helper.writeObject;


before(function (cb) {
    var self = this;

    self.client = testHelper.createClient();
    self.uploads_root = '/' + self.client.user + '/uploads';
    self.root = '/' + self.client.user + '/stor';
    self.dir = self.root + '/' + uuid.v4();
    self.path = self.dir + '/' + uuid.v4();

    self.client.mkdir(self.dir, function (mkdir_err) {
        if (mkdir_err) {
            cb(mkdir_err);
            return;
        } else {
            cb(null);
        }
    });
});


after(function (cb) {
    this.client.rmr(this.dir, cb.bind(null, null));
});


test('abort upload', function (t) {
    var self = this;
    var a = self.client.user;

    createUpload(self, a, self.path, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };
        self.client.abortUpload(o.id, opts, function (err2) {
            if (ifErr(t, err2, 'aborted upload')) {
                t.end();
                return;
            }

            self.client.getUpload(o.id, opts, function (err3, upload) {
                if (ifErr(t, err3, 'got upload')) {
                    t.end();
                    return;
                }

                sanityCheckUpload(t, o, upload);
                t.deepEqual(upload.headers, {});
                t.equal(upload.state, 'finalizing');
                t.equal(upload.type, 'abort');
                t.end();
            });
        });
    });
});

test('abort upload: upload already aborted', function (t) {
    var self = this;
    var a = self.client.user;

    createUpload(self, a, self.path, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }
        checkCreateResponse(t, o);

        var opts = {
            account: a
        };

        self.client.abortUpload(o.id, opts, function (err2) {
            if (ifErr(t, err2, 'aborted upload')) {
                t.end();
                return;
            }

            self.client.abortUpload(o.id, opts, function (err3) {
                if (ifErr(t, err, 'aborted upload')) {
                    t.end();
                    return;
                }

                self.client.getUpload(o.id, opts, function (err4, upload) {
                    if (ifErr(t, err, 'got upload')) {
                        t.end();
                        return;
                    }

                    sanityCheckUpload(t, o, upload);
                    t.deepEqual(upload.headers, {});
                    t.equal(upload.state, 'finalizing');
                    t.equal(upload.type, 'abort');
                    t.end();
                });
            });
        });
    });
});


// Abort: bad input

test('abort upload: upload already committed', function (t) {
    var self = this;
    var a = self.client.user;

    createUpload(self, a, self.path, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };
        self.client.commitUpload(o.id, [], opts, function (err2) {
            if (ifErr(t, err2, 'commited upload')) {
                t.end();
                return;
            }

            self.client.abortUpload(o.id, opts, function (err3) {
                t.ok(err3);
                if (!err3) {
                    return (t.end());
                }
                t.ok(verror.hasCauseWithName(err3,
                    'MultipartUploadFinalizeConflictError'), err);
                t.end();
            });
        });
    });
});


test('abort upload: non-uuid id', function (t) {
    var self = this;
    var opts = {
         account: this.client.user
    };

    var bogus = 'foobar';

    self.client.abortUpload(bogus, opts, function (err, upload) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err, 'ResourceNotFoundError'), err);
        t.end();
    });
});


test('abort upload: non-existent id', function (t) {
    var self = this;
    var opts = {
         account: this.client.user
    };

    var bogus = uuid.v4();

    self.client.getUpload(bogus, opts, function (err, upload) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err, 'ResourceNotFoundError'), err);
        t.end();
    });
});
