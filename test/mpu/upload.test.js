/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var crypto = require('crypto');
var MemoryStream = require('stream').PassThrough;
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
var computePartsMD5 = helper.computePartsMD5;


before(function (cb) {
    helper.initMPUTester.call(this, cb);
});


after(function (cb) {
    helper.cleanupMPUTester.call(this, cb);
});


// TODO streaming object

test('upload part: minimium part number', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.MIN_PART_NUM;
        self.writeTestObject(self.uploadId, pn, function (err2, res) {
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
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.MAX_PART_NUM;
        self.writeTestObject(self.uploadId, pn, function (err2, res) {
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
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.randomPartNum();
        self.writeTestObject(self.uploadId, pn, function (err2, res) {
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
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.randomPartNum();
        var s = new MemoryStream();
        var opts = {
            size: 0
        };
        setImmediate(s.end.bind(s));

        self.client.put(self.uploadPath(pn), s, opts, function (err2, res) {
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
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.MIN_PART_NUM - 1;
        self.writeTestObject(self.uploadId, pn, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.checkResponse(res, 409);
            t.ok(verror.hasCauseWithName(err2,
                'MultipartUploadInvalidArgumentError'));
            t.end();
        });
    });
});


test('upload part: part number greater than allowed', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.MAX_PART_NUM + 1;
        self.writeTestObject(self.uploadId, pn, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.checkResponse(res, 409);
            t.ok(verror.hasCauseWithName(err2,
                'MultipartUploadInvalidArgumentError'));
            t.end();
        });
    });
});


test('upload part: setting durability-level header disallowed', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var h = {
            'durability-level': helper.randomNumCopies()
        };
        var pn = helper.randomPartNum();
        var string = 'foobar';
        var opts = {
            account: self.client.user,
            md5: crypto.createHash('md5').update(string).digest('base64'),
            size: Buffer.byteLength(string),
            type: 'text/plain',
            headers: h
        };

        var s = new MemoryStream();
        self.client.put(self.uploadPath(pn), s, opts, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.checkResponse(res, 409);
            t.ok(verror.hasCauseWithName(err2,
                'MultipartUploadInvalidArgumentError'));
            t.end();
        });
        setImmediate(s.end.bind(s, string));
    });
});


test('upload part: setting x-durability-level header disallowed', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var h = {
            'x-durability-level': helper.randomNumCopies()
        };
        var pn = helper.randomPartNum();
        var string = 'foobar';
        var opts = {
            account: self.client.user,
            md5: crypto.createHash('md5').update(string).digest('base64'),
            size: Buffer.byteLength(string),
            type: 'text/plain',
            headers: h
        };

        var s = new MemoryStream();
        self.client.put(self.uploadPath(pn), s, opts, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.checkResponse(res, 409);
            t.ok(verror.hasCauseWithName(err2,
                'MultipartUploadInvalidArgumentError'));
            t.end();
        });
        setImmediate(s.end.bind(s, string));
    });
});


test('upload part: non-uuid id', function (t) {
    var self = this;
    var bogus = 'foobar';
    var pn = helper.randomPartNum();
    self.uploadId = bogus;

    self.writeTestObject(bogus, pn, function (err, res) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err, 'DirectoryDoesNotExistError'));
        t.checkResponse(res, 404);
        t.end();
    });
});


test('upload part: non-existent id', function (t) {
    var self = this;
    var bogus = uuid.v4();
    var pn = helper.randomPartNum();
    self.uploadId = bogus;

    self.writeTestObject(bogus, pn, function (err, res) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err, 'DirectoryDoesNotExistError'));
        t.checkResponse(res, 404);
        t.end();
    });
});
