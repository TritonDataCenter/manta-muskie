/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2021 Joyent, Inc.
 */

var crypto = require('crypto');
var MemoryStream = require('stream').PassThrough;
var path = require('path');
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


// Subusers (not supported for MPU API)

// Create
test('subusers disallowed: create', function (t) {
    var self = this;
    self.createUploadSubuser(self.path, {}, function (err) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'AuthorizationFailedError'), err);
        t.end();
    });
});

// Get
test('subusers disallowed: get upload under same account', function (t) {
    var self = this;
    self.createUpload(self.path, {}, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.getUploadSubuser(self.uploadId, function (err2, upload) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2, 'AuthorizationFailedError'),
                err2);
            t.end();
        });
    });
});


// Upload
test('subusers disallowed: upload part', function (t) {
    var self = this;
    self.createUpload(self.path, {}, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.randomPartNum();
        self.writeTestObjectSubuser(self.uploadId, pn, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2, 'NoMatchingRoleTagError'),
                err2);
            t.end();
        });
    });
});


// Abort
test('subusers disallowed: abort', function (t) {
    var self = this;
    self.createUpload(self.path, {}, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.abortUploadSubuser(self.uploadId, function (err2) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2, 'AuthorizationFailedError'),
                err2);
            t.end();
        });
    });
});


// Commit
test('subusers disallowed: commit', function (t) {
    var self = this;
    self.createUpload(self.path, {}, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.commitUploadSubuser(self.uploadId, [], function (err2) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2, 'AuthorizationFailedError'),
                err2);
            t.end();
        });
    });
});


// Redirect
test('subusers disallowed: redirect (GET /:account/uploads/:id)', function (t) {
    var self = this;
    self.createUpload(self.path, {}, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            account: self.client.user
        };

        self.userClient.get(self.redirectPath(), opts, function (err2) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2, 'AuthorizationFailedError'),
                err2);
            t.end();
        });
    });
});


test('subusers disallowed: redirect (GET /:account/uploads/:id/:partNum)',
function (t) {
    var self = this;
    self.createUpload(self.path, {}, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            account: self.client.user
        };

        var pn = 0;
        self.userClient.get(self.redirectPath(pn), opts, function (err2) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2, 'AuthorizationFailedError'),
                err2);
            t.end();
        });
    });
});


// Forbidden routes
test('PUT /:account/uploads disallowed', function (t) {
    var self = this;
    var a = self.client.user;
    var p = '/' + a + '/uploads';

    var string = 'foobar';
    var opts = {
        account: a,
        md5: crypto.createHash('md5').update(string).digest('base64'),
        size: Buffer.byteLength(string),
        type: 'text/plain'
    };
    var stream = new MemoryStream();
    setImmediate(stream.end.bind(stream, string));
    self.client.put(p, stream, opts, function (err, res) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.checkResponse(res, 400);
        t.ok(verror.hasCauseWithName(err,
            'OperationNotAllowedOnRootDirectoryError'), err);
        t.end();
    });
});


test('POST /:account/uploads/[0-f]/:id disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            headers: {
                'content-type': 'application/json',
                'accept': 'application/json'
            },
            path: self.uploadPath()
        };

        self.client.signRequest({
            headers: opts.headers
        }, function (err2) {
            if (ifErr(t, err2, 'write test object')) {
                t.end();
                return;
            }

            self.client.jsonClient.post(opts, {}, function (err3, _, res) {
                t.ok(err3);
                if (!err3) {
                    return (t.end());
            }
                t.checkResponse(res, 405);
                t.ok(verror.hasCauseWithName(err3, 'MethodNotAllowedError'),
                    err3);
                t.end();
            });
        });
    });
});


test('DELETE /:account/uploads/[0-f]/:id disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.client.unlink(self.uploadPath(), function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.checkResponse(res, 405);
            t.ok(verror.hasCauseWithName(err2, 'MethodNotAllowedError'), err2);
            t.end();
        });
    });
});


test('HEAD /:account/uploads/[0-f]/:id/state disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var action = 'state';
        var p = self.uploadPath() + '/' + action;
        self.client.info(p, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.checkResponse(res, 405);
            t.ok(verror.hasCauseWithName(err2, 'MethodNotAllowedError'), err2);
            t.end();
        });
    });
});


test('PUT /:account/uploads/[0-f]/:id/state disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var action = 'state';
        var p = self.uploadPath() + '/' + action;
        var s = new MemoryStream();
        var opts = {
            size: 0
        };
        setImmediate(s.end.bind(s));

       self.client.put(p, s, opts, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            // For PUTS, muskie interprets "state" as the partNum
            t.checkResponse(res, 409);
            t.ok(verror.hasCauseWithName(err2,
                'MultipartUploadInvalidArgumentError'), err2);
            t.end();
        });
    });
});


test('POST /:account/uploads/[0-f]/:id/state disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var action = 'state';
        var p = self.uploadPath() + '/' + action;
        var opts = {
            headers: {
                'content-type': 'application/json',
                'accept': 'application/json'
            },
            path: p
        };

        self.client.signRequest({
            headers: opts.headers
        }, function (err2) {
            if (ifErr(t, err2, 'write test object')) {
                t.end();
                return;
            }

            self.client.jsonClient.post(opts, {}, function (err3, _, res) {
                t.ok(err3);
                if (!err3) {
                    return (t.end());
                }
                t.checkResponse(res, 405);
                t.ok(verror.hasCauseWithName(err3, 'MethodNotAllowedError'),
                    err3);
                t.end();
            });
        });
    });
});


test('DELETE /:account/uploads/[0-f]/:id/state disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var action = 'state';
        var p = self.uploadPath() + '/' + action;
        self.client.unlink(p, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.checkResponse(res, 405);
            t.ok(verror.hasCauseWithName(err2, 'MethodNotAllowedError'), err2);
            t.end();
        });
    });
});


test('PUT /:account/uploads/[0-f]/:id/abort disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var action = 'abort';
        var p = self.uploadPath() + '/' + action;
        var s = new MemoryStream();
        var opts = {
            size: 0
        };
        setImmediate(s.end.bind(s));

        self.client.put(p, s, opts, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            // For PUTS, muskie interprets "abort" as the partNum
            t.checkResponse(res, 409);
            t.ok(verror.hasCauseWithName(err2,
                'MultipartUploadInvalidArgumentError'), err2);
            t.end();
        });
    });
});


test('GET /:account/uploads/[0-f]/:id/abort disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var action = 'abort';
        var p = self.uploadPath() + '/' + action;
        self.client.get(p, function (err2, _, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.checkResponse(res, 405);
            t.ok(verror.hasCauseWithName(err2, 'MethodNotAllowedError'), err2);
            t.end();
        });
    });
});


test('HEAD /:account/uploads/[0-f]/:id/abort disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var action = 'abort';
        var p = self.uploadPath() + '/' + action;
        self.client.info(p, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.checkResponse(res, 405);
            t.ok(verror.hasCauseWithName(err2, 'MethodNotAllowedError'), err2);
            t.end();
        });
    });
});


test('DELETE /:account/uploads/[0-f]/:id/abort disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var action = 'abort';
        var p = self.uploadPath() + '/' + action;
        self.client.unlink(p, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.checkResponse(res, 405);
            t.ok(verror.hasCauseWithName(err2, 'MethodNotAllowedError'), err2);
            t.end();
        });
    });
});


test('GET /:account/uploads/[0-f]/:id/commit disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var action = 'commit';
        var p = self.uploadPath() + '/' + action;
        self.client.get(p, {}, function (err2, _, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.checkResponse(res, 405);
            t.ok(verror.hasCauseWithName(err2, 'MethodNotAllowedError'), err2);
            t.end();
        });
    });
});


test('HEAD /:account/uploads/[0-f]/:id/commit disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var action = 'commit';
        var p = self.uploadPath() + '/' + action;
        self.client.info(p, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.checkResponse(res, 405);
            t.ok(verror.hasCauseWithName(err2, 'MethodNotAllowedError'), err2);
            t.end();
        });
    });
});


test('PUT /:account/uploads/[0-f]/:id/commit disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var action = 'commit';
        var p = self.uploadPath() + '/' + action;
        var s = new MemoryStream();
        var opts = {
            size: 0
        };
        setImmediate(s.end.bind(s));

        self.client.put(p, s, opts, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            // For PUTS, muskie interprets "commit" as the partNum
            t.checkResponse(res, 409);
            t.ok(verror.hasCauseWithName(err2,
                'MultipartUploadInvalidArgumentError'), err2);
            t.end();
        });
    });
});


test('DELETE /:account/uploads/[0-f]/:id/commit disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var action = 'commit';
        var p = self.uploadPath() + '/' + action;
        self.client.unlink(p, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.checkResponse(res, 405);
            t.ok(verror.hasCauseWithName(err2, 'MethodNotAllowedError'), err2);
            t.end();
        });
    });
});


test('GET /:account/uploads/[0-f]/:id/:partNum disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.randomPartNum();
        self.writeTestObject(self.uploadId, pn, function (err2) {
            self.client.get(self.uploadPath(pn), {}, function (err3, _, res) {
                t.ok(err3);
                if (!err3) {
                    return (t.end());
                }
                t.checkResponse(res, 405);
                t.ok(verror.hasCauseWithName(err3, 'MethodNotAllowedError'),
                    err3);
                t.end();
            });
        });
    });
});


test('POST /:account/uploads/[0-f]/:id/:partNum disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.randomPartNum();
        self.writeTestObject(self.uploadId, pn, function (err2) {
            if (ifErr(t, err2, 'write test object')) {
                t.end();
                return;
            }

            var opts = {
                headers: {
                    'content-type': 'application/json',
                    'accept': 'application/json'
                },
                path: self.uploadPath(pn)
            };

            self.client.signRequest({
                headers: opts.headers
            }, function (err3) {
                if (ifErr(t, err3, 'write test object')) {
                    t.end();
                    return;
                }

                self.client.jsonClient.post(opts, {}, function (err4, _, res) {
                    t.ok(err4);
                    if (!err4) {
                        return (t.end());
                    }
                    t.checkResponse(res, 405);
                    t.ok(verror.hasCauseWithName(err4, 'MethodNotAllowedError'),
                        err4);
                    t.end();
                });
            });
        });
    });
});


test('DELETE /:account/uploads/[0-f]/:id/:partNum disallowed', function (t) {
    var self = this;

    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.randomPartNum();
        self.writeTestObject(self.uploadId, pn, function (err2) {
            self.client.unlink(self.uploadPath(pn), function (err3, res) {
                t.ok(err3);
                if (!err3) {
                    return (t.end());
                }
                t.checkResponse(res, 405);
                t.ok(verror.hasCauseWithName(err3, 'MethodNotAllowedError'),
                    err3);
                t.end();
            });
        });
    });
});
