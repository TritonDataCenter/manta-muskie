/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var path = require('path');
var uuid = require('node-uuid');
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


// Create: happy cases
test('create upload', function (t) {
    var self = this;
    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.getUpload(self.uploadId, function (err2, upload) {
            if (ifErr(t, err2, 'got upload')) {
                t.end();
                return;
            }

            t.deepEqual(upload.headers, h);
            t.ok(upload.state, 'created');
            t.end();
        });
    });
});

// Verifies that time-stamp set in the upload record
// upon mpu creation exists and has a reasonable value.
test('create upload: upload record creation time', function (t) {
    var self = this;
    var h = {};
    var beforeUpload = Date.now();
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var afterUpload = Date.now();

        self.getUpload(self.uploadId, function (err2, upload) {
            if (ifErr(t, err2, 'got upload')) {
                t.end();
                return;
            }
            t.ok(upload.creationTimeMs);
            t.ok(beforeUpload < upload.creationTimeMs, 'before time check');
            t.ok(upload.creationTimeMs < afterUpload, 'after time check');
            t.end();
        });
    });
});

// content-length
test('create upload: content-length header', function (t) {
    var self = this;
    var size = helper.randomUploadSize();
    var h = {
        'content-length': size
    };

    self.createUpload(self.path, h, function (err, o) {
         if (ifErr(t, err, 'created upload')) {
             t.end();
             return;
         }

        self.getUpload(self.uploadId, function (err2, upload) {
             if (ifErr(t, err2, 'got upload')) {
                 t.end();
                 return;
             }

             t.deepEqual(upload.headers, h);
             t.ok(upload.state, 'created');
             t.end();
        });
    });
});

// durability-level
test('create upload: durability-level header', function (t) {
    var self = this;
    var copies = helper.randomNumCopies();

    var h = {
        'durability-level': copies
    };

    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.getUpload(self.uploadId, function (err2, upload) {
            if (ifErr(t, err2, 'got upload')) {
                t.end();
                return;
            }

            t.deepEqual(upload.headers, h);
            t.ok(upload.state, 'created');
            t.end();
        });
    });
});


// x-durability-level
test('create upload: x-durability-level header', function (t) {
    var self = this;
    var copies = helper.randomNumCopies();
    var h = {
        'x-durability-level': copies
    };

    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'got upload')) {
            t.end();
            return;
        }

        self.getUpload(self.uploadId, function (err2, upload) {
            if (ifErr(t, err2, 'got upload')) {
                t.end();
                return;
            }

            t.deepEqual(upload.headers, h);
            t.ok(upload.state, 'created');
            t.end();
        });
    });
});


// content-md5
test('create upload: content-md5 header', function (t) {
    var self = this;
    var h = {
        'content-md5': 'JdMoQCNCYOHEGq1fgaYyng=='
    };

    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.getUpload(self.uploadId, function (err2, upload) {
            t.ifError(err2);
            if (ifErr(t, err, 'created upload')) {
                t.end();
                return;
            }

            t.deepEqual(upload.headers, h);
            t.ok(upload.state, 'created');
            t.end();
        });
    });
});


// mix of headers, supported and unsupported
test('create upload: various headers', function (t) {
    var self = this;
    var copies = helper.randomNumCopies();
    var size = helper.randomUploadSize();

    var h = {
        'content-length': size,
        'durability-level': copies,
        'content-md5': 'JdMoQCNCYOHEGq1fgaYyng==',
        'm-my-custom-header': 'my-custom-value'
    };

    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.getUpload(self.uploadId, function (err2, upload) {
            if (ifErr(t, err2, 'got upload')) {
                t.end();
                return;
            }

            t.deepEqual(upload.headers, h);
            t.ok(upload.state, 'created');
            t.end();
        });
    });
});


// make sure headers are case-insensitive
test('create upload: mixed case headers', function (t) {
    var self = this;
    var copies = helper.randomNumCopies();
    var size = helper.randomUploadSize();
    var h = {
        'Content-Length': size,
        'DURABILITY-LEVEL': copies,
        'cOntEnt-Md5': 'JdMoQCNCYOHEGq1fgaYyng==',
        'm-my-CuStoM-header': 'my-custom-value'
    };

    // only headers should be case-insensitive (values shouldn't change)
    var lowerCase = {
        'content-length': size,
        'durability-level': copies,
        'content-md5': 'JdMoQCNCYOHEGq1fgaYyng==',
        'm-my-custom-header': 'my-custom-value'
    };

    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.getUpload(self.uploadId, function (err2, upload) {
            if (ifErr(t, err2, 'got upload')) {
                t.end();
                return;
            }

            t.deepEqual(upload.headers, lowerCase);
            t.ok(upload.state, 'created');
            t.end();
        });
    });
});


// Create: bad input

test('create upload: no input object path', function (t) {
    var self = this;

    self.createUpload(null, null, function (err, o) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'MultipartUploadInvalidArgumentError'), err);
        t.end();
    });
});

test('create upload: object path under a nonexistent account', function (t) {
    var self = this;
    var bogus = uuid.v4();
    var p = '/' + bogus + '/foo.txt';

    self.createUpload(p, null, function (err) {
        if (!err) {
            t.fail('upload created under a different account');
            t.end();
            return;
        }

        t.ok(verror.hasCauseWithName(err, 'AccountDoesNotExistError'));
        t.end();
    });
});


test('create upload: object path not a string', function (t) {
    var self = this;

    self.createUpload([], null, function (err, o) {
        t.ok(err); //TODO error message name
        if (!err) {
            return (t.end());
        }
        t.end();
    });
});


test('create upload: if-match header disalowed', function (t) {
    var self = this;
    var h = {
        'if-match': 'foo'
    };

    self.createUpload(self.path, h, function (err, o) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'MultipartUploadInvalidArgumentError'), err);
        t.end();
    });
});


test('create upload: if-none-match header disalowed', function (t) {
    var self = this;
    var h = {
        'if-none-match': 'foo'
    };

    self.createUpload(self.path, h, function (err, o) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'MultipartUploadInvalidArgumentError'), err);
        t.end();
    });
});


test('create upload: if-modified-since header disalowed', function (t) {
    var self = this;
    var h = {
        'if-modified-since': 'foo'
    };

    self.createUpload(self.path, h, function (err, o) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'MultipartUploadInvalidArgumentError'), err);
        t.end();
    });
});

test('create upload: if-unmodified-since header disalowed', function (t) {
    var self = this;
    var h = {
        'if-unmodified-since': 'foo'
    };

    self.createUpload(self.path, h, function (err, o) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'MultipartUploadInvalidArgumentError'), err);
        t.end();
    });
});


test('create upload: content-length less than allowed', function (t) {
    var self = this;
    var size = helper.MIN_UPLOAD_SIZE - 1;
    var h = {
        'content-length': size
    };

    self.createUpload(self.path, h, function (err, o) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'MultipartUploadInvalidArgumentError'), err);
        t.end();
    });
});


test('create upload: durability-level greater than allowed', function (t) {
    var self = this;
    var copies = helper.MAX_NUM_COPIES + 1;
    var h = {
        'durability-level': copies
    };

    self.createUpload(self.path, h, function (err, o) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'InvalidDurabilityLevelError'), err);
        t.end();
    });
});

test('create upload: x-durability-level greater than allowed', function (t) {
    var self = this;
    var copies = helper.MAX_NUM_COPIES + 1;
    var h = {
        'x-durability-level': copies
    };

    self.createUpload(self.path, h, function (err, o) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'InvalidDurabilityLevelError'), err);
        t.end();
    });
});


test('create upload: durability-level less than allowed', function (t) {
    var self = this;
    var copies = helper.MIN_NUM_COPIES - 1;
    var h = {
        'durability-level': copies
    };

    self.createUpload(self.path, h, function (err, o) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'InvalidDurabilityLevelError'), err);
        t.end();
    });
});


test('create upload: x-durability-level less than allowed', function (t) {
    var self = this;
    var copies = helper.MIN_NUM_COPIES - 1;
    var h = {
        'x-durability-level': copies
    };

    self.createUpload(self.path, h, function (err, o) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'InvalidDurabilityLevelError'), err);
        t.end();
    });
});

// content-disposition
test('create upload: content-disposition header', function (t) {
    var self = this;
    var cd = 'attachment; filename="my-file.txt"';
    var h = {
        'content-disposition': cd
    };

    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.getUpload(self.uploadId, function (err2, upload) {
            if (ifErr(t, err2, 'got upload')) {
                t.end();
                return;
            }

            t.deepEqual(upload.headers, h, 'created headers match');
            t.ok(upload.state, 'created');
            t.end();
        });
    });
});


test('create upload: invalid content-disposition', function (t) {
    var self = this;
    var h = {
        'content-disposition': 'attachment;'
    };

    self.createUpload(self.path, h, function (err, o) {
        t.ok(err, 'Expect error');
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'BadRequestError'), 'Expected 400');
        t.end();
    });
});
