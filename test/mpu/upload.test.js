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


// TODO streaming object
// TODO size greater than max part size

test('upload part: minimium part number', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var pn = helper.MIN_PART_NUM;
        var opts = createPartOptions(a, helper.TEXT);

        writeObject(self.client, o.id, pn, opts, function (err2, res) {
            if (ifErr(t, err, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);
            t.end();
        });
    });
});


test('upload part: maximum part number', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var pn = helper.MAX_PART_NUM;
        var opts = createPartOptions(a, helper.TEXT);

        writeObject(self.client, o.id, pn, opts, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);
            t.end();
        });
    });
});


test('upload part: random part number', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var pn = helper.randomPartNum();
        var opts = createPartOptions(a, helper.TEXT);

        writeObject(self.client, o.id, pn, opts, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);
            t.end();
        });
    });
});


test('upload part: zero-byte part', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var pn = helper.randomPartNum();
        var opts = createPartOptions(a, helper.TEXT);
        opts.size = 0;

        writeObject(self.client, o.id, pn, opts, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);
            t.end();
        });
    });
});


// Upload: bad input

test('upload part: part number less than allowed', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var pn = helper.MIN_PART_NUM - 1;
        var opts = createPartOptions(a, helper.TEXT);

        writeObject(self.client, o.id, pn, opts, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2, 'MultipartUploadPartNumError'),
                err);
            t.end();
        });
    });
});


test('upload part: part number greater than allowed', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }
        checkCreateResponse(t, o);

        var pn = helper.MAX_PART_NUM + 1;
        var opts = createPartOptions(a, helper.TEXT);

        writeObject(self.client, o.id, pn, opts, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2, 'MultipartUploadPartNumError'),
                err);
            t.end();
        });
    });
});


test('upload part: non-uuid id', function (t) {
    var self = this;

    var bogus = 'foobar';
    var pn = helper.randomPartNum();
    var opts = createPartOptions(this.client.user, helper.TEXT);

    writeObject(self.client, bogus, pn, opts, function (err, res) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err, 'DirectoryDoesNotExistError'), err);
        t.checkResponse(res, 404);
        t.end();
    });
});


test('upload part: non-existent id', function (t) {
    var self = this;

    var bogus = uuid.v4();
    var pn = helper.randomPartNum();
    var opts = createPartOptions(this.client.user, helper.TEXT);

    writeObject(self.client, bogus, pn, opts, function (err, res) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err, 'DirectoryDoesNotExistError'), err);
        t.checkResponse(res, 404);
        t.end();
    });
});
