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

var obj = require('../../lib/obj.js');

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


// Create: happy cases

test('create upload', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;
    var h = {};

    createUpload(self, a, p, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };
        self.client.getUpload(o.id, opts, function (err2, upload) {
            if (ifErr(t, err2, 'got upload')) {
                t.end();
                return;
            }

            sanityCheckUpload(t, o, upload);
            t.deepEqual(upload.headers, h);
            t.ok(upload.state, 'created');
            t.end();
        });
    });
});


// content-length
test('create upload: content-length header', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    var size = helper.randomUploadSize();
    var h = {
        'content-length': size
    };

    createUpload(self, a, p, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };
        self.client.getUpload(o.id, opts, function (err2, upload) {
            if (ifErr(t, err2, 'got upload')) {
                t.end();
                return;
            }

            sanityCheckUpload(t, o, upload);
            t.deepEqual(upload.headers, h);
            t.ok(upload.state, 'created');
            t.end();
        });
    });
});


// durability-level
test('create upload: durability-level header', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;
    var copies = helper.randomNumCopies();

    var h = {
        'durability-level': copies
    };

    createUpload(self, a, p, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };
        self.client.getUpload(o.id, opts, function (err2, upload) {
            if (ifErr(t, err2, 'got upload')) {
                t.end();
                return;
            }

            sanityCheckUpload(t, o, upload);
            t.deepEqual(upload.headers, h);
            t.ok(upload.state, 'created');
            t.end();
        });
    });
});


// x-durability-level
test('create upload: x-durability-level header', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    var copies = helper.randomNumCopies();

    var h = {
        'x-durability-level': copies
    };

    createUpload(self, a, p, h, function (err, o) {
        if (ifErr(t, err, 'got upload')) {
            t.end();
            return;
        }
        checkCreateResponse(t, o);

        var opts = {
            account: a
        };

        self.client.getUpload(o.id, opts, function (err2, upload) {
            if (ifErr(t, err2, 'got upload')) {
                t.end();
                return;
            }

            sanityCheckUpload(t, o, upload);
            t.deepEqual(upload.headers, h);
            t.ok(upload.state, 'created');
            t.end();
        });
    });
});


// content-md5
test('create upload: content-md5 header', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    var h = {
        'content-md5': 'JdMoQCNCYOHEGq1fgaYyng=='
    };

    createUpload(self, a, p, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };
        self.client.getUpload(o.id, opts, function (err2, upload) {
            t.ifError(err2);
            if (ifErr(t, err, 'created upload')) {
                t.end();
                return;
            }

            sanityCheckUpload(t, o, upload);
            t.deepEqual(upload.headers, h);
            t.ok(upload.state, 'created');
            t.end();
        });
    });
});


// mix of headers, supported and unsupported
test('create upload: various headers', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    var copies = helper.randomNumCopies();
    var size = helper.randomUploadSize();

    var h = {
        'content-length': size,
        'durability-level': copies,
        'content-md5': 'JdMoQCNCYOHEGq1fgaYyng==',
        'm-my-custom-header': 'my-custom-value'
    };

    createUpload(self, a, p, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };
        self.client.getUpload(o.id, opts, function (err2, upload) {
            if (ifErr(t, err2, 'got upload')) {
                t.end();
                return;
            }

            sanityCheckUpload(t, o, upload);
            t.deepEqual(upload.headers, h);
            t.ok(upload.state, 'created');
            t.end();
        });
    });
});


// make sure headers are case-insensitive
test('create upload: mixed case headers', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

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

    createUpload(self, a, p, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };
        self.client.getUpload(o.id, opts, function (err2, upload) {
            if (ifErr(t, err2, 'got upload')) {
                t.end();
                return;
            }

            sanityCheckUpload(t, o, upload);
            t.deepEqual(upload.headers, lowerCase);
            t.ok(upload.state, 'created');
            t.end();
        });
    });
});


// Create: bad input

test('create upload: no input object path', function (t) {
    var self = this;
    var a = self.client.user;

    createUpload(self, a, null, null, function (err, o) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'MultipartUploadMissingObjecPathError'), err);
        t.end();
    });
});


test('create upload: object path not a string', function (t) {
    var self = this;
    var a = self.client.user;

    createUpload(self, a, [], null, function (err, o) {
        t.ok(err); //TODO error message name
        if (!err) {
            return (t.end());
        }
        t.end();
    });
});


test('create upload: content-length greater than allowed', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    var size = obj.DEF_MAX_LEN + 1;
    var h = {
        'content-length': size
    };

    createUpload(self, a, p, h, function (err, o) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'InvalidMaxContentLengthError'), err);
        t.end();
    });
});


test('create upload: content-length less than allowed', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    var size = helper.MIN_UPLOAD_SIZE - 1;
    var h = {
        'content-length': size
    };

    createUpload(self, a, p, h, function (err, o) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'InvalidMaxContentLengthError'), err);
        t.end();
    });
});


test('create upload: durability-level greater than allowed', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    var copies = helper.MAX_NUM_COPIES + 1;
    var h = {
        'durability-level': copies
    };

    createUpload(self, a, p, h, function (err, o) {
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
    var a = self.client.user;
    var p = self.dir;

    var copies = helper.MAX_NUM_COPIES + 1;
    var h = {
        'x-durability-level': copies
    };

    createUpload(self, a, p, h, function (err, o) {
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
    var a = self.client.user;
    var p = self.dir;

    var copies = helper.MIN_NUM_COPIES - 1;
    var h = {
        'durability-level': copies
    };

    createUpload(self, a, p, h, function (err, o) {
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
    var a = self.client.user;
    var p = self.dir;

    var copies = helper.MIN_NUM_COPIES - 1;
    var h = {
        'x-durability-level': copies
    };

    createUpload(self, a, p, h, function (err, o) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'InvalidDurabilityLevelError'), err);
        t.end();
    });
});
