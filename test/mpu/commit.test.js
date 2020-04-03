/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var MemoryStream = require('stream').PassThrough;
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


before(function (cb) {
    helper.initMPUTester.call(this, cb);
});


after(function (cb) {
    helper.cleanupMPUTester.call(this, cb);
});

// Commit

test('commit upload: zero parts', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.commitUpload(self.uploadId, [], function (err3) {
            if (ifErr(t, err3, 'committed upload')) {
                t.end();
                return;
            }

            self.getUpload(self.uploadId, function (err4, upload) {
                if (ifErr(t, err4, 'created upload')) {
                    t.end();
                    return;
                }

                t.deepEqual(upload.headers, {});
                t.equal(upload.state, 'done');
                t.equal(upload.result, 'committed');
                t.equal(upload.partsMD5Summary, computePartsMD5([]));
                t.end();
            });
        });
    });
});


test('commit upload: one part', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = 0;
        self.writeTestObject(self.uploadId, pn, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            var etag = res.headers.etag;
            self.commitUpload(self.uploadId, [etag], function (err3) {
                if (ifErr(t, err3, 'committed upload')) {
                    t.end();
                    return;
                }

                self.getUpload(self.uploadId, function (err4, upload) {
                    if (ifErr(t, err4, 'created upload')) {
                        t.end();
                        return;
                    }

                    t.deepEqual(upload.headers, {});
                    t.equal(upload.state, 'done');
                    t.equal(upload.result, 'committed');
                    t.equal(upload.partsMD5Summary, computePartsMD5([etag]));
                    t.end();
                });
            });
        });
    });
});

test('commit upload: commit content md5 match', function (t) {
    var self = this;
    vasync.waterfall([
        function (callback) {
            self.createUpload(self.path, null, function (err) {
                if (ifErr(t, err, 'created upload')) {
                    callback(err);
                } else {
                    callback(null);
                }
            });
        },
        function (callback) {
            self.writeTestObject(self.uploadId, 0, function (err, res) {
                if (ifErr(t, err, 'uploaded part')) {
                    callback(err);
                } else {
                    t.ok(res);
                    t.checkResponse(res, 204);
                    callback(null, res.headers.etag);
                }
            });
        },
        function (etag, callback) {
            self.commitUpload(self.uploadId, [etag], function (err, res) {
                if (ifErr(t, err, 'commited upload')) {
                    callback(err);
                } else {
                    t.ok(res);
                    t.ok(res.headers);
                    t.ok(res.headers['computed-md5']);
                    if (res === undefined || res.headers == undefined) {
                        callback(new Error('commit upload missing res'));
                        return;
                    }
                    callback(null, res.headers['computed-md5']);
                }
            });
        },
        function (computedMd5, callback) {
            self.client.info(self.path, function (err, info) {
                if (ifErr(t, err, 'got object info')) {
                    callback(err);
                } else {
                    t.ok(info, 'failed to get object info');
                    if (info) {
                        var headers = info.headers || {};
                        t.equal(computedMd5, helper.TEXT_MD5);
                        t.equal(computedMd5, headers['content-md5']);
                    }
                    callback(null);
                }
            });
        }
    ], function () {
        t.end();
    });
});


test('commit upload: already commited, same set of parts', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = 0;
        self.writeTestObject(self.uploadId, pn, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            var etag = res.headers.etag;
            self.commitUpload(self.uploadId, [etag], function (err3) {
                if (ifErr(t, err3, 'committed upload')) {
                    t.end();
                    return;
                }

                self.commitUpload(self.uploadId, [etag], function (err4) {
                    if (ifErr(t, err4, 'committed upload')) {
                        t.end();
                        return;
                    }

                    self.getUpload(self.uploadId, function (err5, upload) {
                        if (ifErr(t, err5, 'got upload')) {
                            t.end();
                            return;
                        }

                        t.deepEqual(upload.headers, {});
                        t.equal(upload.state, 'done');
                        t.equal(upload.result, 'committed');
                        t.equal(upload.partsMD5Summary,
                            computePartsMD5([etag]));
                        t.end();
                    });
                });
            });
        });
    });
});


test('commit upload: ensure etag exists on target object', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.commitUpload(self.uploadId, [], function (err2) {
            if (ifErr(t, err2, 'committed upload')) {
                t.end();
                return;
            }

            self.client.info(self.path, function (err3, info1) {
                if (ifErr(t, err3, 'HEAD target object')) {
                    t.end();
                    return;
                }

                t.ok(info1);
                t.ok(info1.etag);

                var s = new MemoryStream();
                var string = 'foobar';
                var opts = {
                    size: string.length
                };
                setImmediate(s.end.bind(s, string));

                self.client.put(self.path, s, opts, function (err4, res) {
                    if (ifErr(t, err4, 'overwriting target object')) {
                        t.end();
                        return;
                    }

                    self.client.info(self.path, function (err5, info2) {
                        if (ifErr(t, err5, 'HEAD overwritten target object')) {
                            t.end();
                            return;
                        }

                        t.ok(info2);
                        t.ok(info2.etag);
                        t.ok(info1.etag !== info2.etag);
                        t.done();
                    });
                });
            });
        });
    });
});




// Commit: invalid upload (not related to the JSON API inputs)

test('commit upload: already aborted', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.abortUpload(self.uploadId, function (err2) {
            if (ifErr(t, err2, 'created upload')) {
                t.end();
                return;
            }

            self.commitUpload(self.uploadId, [], function (err3) {
                if (!err3) {
                    t.fail('upload already aborted');
                    t.end();
                    return;
                }

                t.ok(verror.hasCauseWithName(err3,
                    'InvalidMultipartUploadStateError'));
                t.end();
            });
        });
    });
});


test('commit upload: already committed, different set of parts', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = 0;
        self.writeTestObject(self.uploadId, pn, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            var etag = res.headers.etag;
            self.commitUpload(self.uploadId, [etag, etag], function (err3) {
                if (!err3) {
                    t.fail('upload already committed with different part set');
                    t.end();
                    return;
                }

                t.ok(verror.hasCauseWithName(err3,
                    'MultipartUploadInvalidArgumentError'));
                t.end();
            });
        });
    });
});


test('commit upload: object size does not match create header (0 parts)',
function (t) {
    var self = this;
    var h = {
        'content-length': helper.TEXT.length
    };

    self.createUpload(self.path, h, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.commitUpload(self.uploadId, [], function (err2) {
            if (!err2) {
                t.fail('object size mismatch');
                t.end();
                return;
            }

            t.ok(verror.hasCauseWithName(err2,
                'MultipartUploadInvalidArgumentError'));
            t.end();
        });
    });

});


test('commit upload: object size does not match create header (1 part)',
function (t) {
    var self = this;
    var h = {
        'content-length': helper.TEXT.length + 1
    };
    self.createUpload(self.path, h, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = 0;
        self.writeTestObject(self.uploadId, pn, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            var etag = res.headers.etag;
            self.commitUpload(self.uploadId, [etag], function (err3) {
                if (!err3) {
                    t.fail('object size mismatch');
                    t.end();
                    return;
                }

                t.ok(verror.hasCauseWithName(err3,
                    'MultipartUploadInvalidArgumentError'));
                t.end();
            });
        });
    });
});


test('commit upload: content-md5 does not match create header (0 parts)',
function (t) {
    var self = this;
    var h = {
        'content-md5': helper.TEXT_MD5
    };

    self.createUpload(self.path, h, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.commitUpload(self.uploadId, [], function (err2) {
            if (!err2) {
                t.fail('content MD5 mismatch');
                t.end();
                return;
            }

            t.ok(verror.hasCauseWithName(err2,
                'MultipartUploadInvalidArgumentError'));
            t.end();
        });
    });

});


test('commit upload: content-md5 does not match create header (1 part)',
function (t) {
    var self = this;
    var h = {
        'content-md5': helper.ZERO_BYTE_MD5
    };

    self.createUpload(self.path, h, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = 0;
        self.writeTestObject(self.uploadId, pn, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            var etag = res.headers.etag;
            self.commitUpload(self.uploadId, [etag], function (err3) {
                if (!err3) {
                    t.fail('content MD5 mismatch');
                    t.end();
                    return;
                }

                t.ok(verror.hasCauseWithName(err3,
                    'MultipartUploadInvalidArgumentError'));
                t.end();
            });
        });
    });
});



test('commit upload: non-final part less than min part size', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var etags = [];
        vasync.forEachParallel({
            func: function uploadText(pn, cb) {
                self.writeTestObject(self.uploadId, pn, function (errw, res) {
                    if (!errw) {
                        etags[pn] = res.headers.etag;
                    }
                    cb();
                });
            },
            inputs: [0, 1, 2]
        }, function (errp, results) {
            if (ifErr(t, errp, 'uploading parts')) {
                t.end();
                return;
            }

            self.commitUpload(self.uploadId, etags, function (err2) {
                if (!err2) {
                    t.fail('non-final part has less than minimum size');
                    t.end();
                    return;
                }

                t.ok(verror.hasCauseWithName(err2,
                    'MultipartUploadInvalidArgumentError'));
                t.end();
            });
        });
    });
});


// Commit: invalid object path specifed on create

test('commit upload: path is top-level directory', function (t) {
    var self = this;
    var p = '/' + self.client.user + '/stor';

    self.createUpload(p, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.commitUpload(self.uploadId, [], function (err2) {
            if (!err2) {
                t.fail('invalid object path (top-level directory)');
                t.end();
                return;
            }

            t.ok(verror.hasCauseWithName(err2,
                'OperationNotAllowedOnDirectoryError'));
             t.end();
        });
    });
});


test('commit upload: parent dir does not exist (parent is top-level dir)',
function (t) {
    var self = this;
    var p = '/' + self.client.user + '/' + uuid.v4() + '/foo.txt';

    self.createUpload(p, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.commitUpload(self.uploadId, [], function (err2) {
            if (!err2) {
                t.fail('invalid object path (parent is top-level directory)');
                t.end();
                return;
            }

            t.ok(verror.hasCauseWithName(err2, 'DirectoryDoesNotExistError'));
             t.end();
        });
    });
});


test('commit upload: parent dir does not exist (parent is not a top-level dir)',
function (t) {
    var self = this;
    var p = self.dir + '/foobar/foo.txt';

    self.createUpload(p, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.commitUpload(self.uploadId, [], function (err2) {
            if (!err2) {
                t.fail('invalid object path (parent does not exist)');
                t.end();
                return;
            }

            t.ok(verror.hasCauseWithName(err2,
                'DirectoryDoesNotExistError'));
             t.end();
        });
    });
});


test('commit upload: object path under another account', function (t) {
    var self = this;
    var p = '/poseidon/stor/foo.txt';

    self.createUpload(p, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.commitUpload(self.uploadId, [], function (err2) {
            if (!err2) {
                t.fail('upload created under a different account');
                t.end();
                return;
            }

            t.ok(verror.hasCauseWithName(err2,
                'AuthorizationFailedError'));
            t.end();
        });
    });
});


// Commit: bad inputs to API

test('commit upload: empty part etag', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = 0;
        self.writeTestObject(self.uploadId, pn, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            self.commitUpload(self.uploadId, [''], function (err3) {
                if (!err3) {
                    t.fail('commit part 0 has an empty etag');
                    t.end();
                    return;
                }

                t.ok(verror.hasCauseWithName(err3,
                    'MultipartUploadInvalidArgumentError'));
                t.end();
            });
        });
    });
});


test('commit upload: multiple empty part etags', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = 0;
        self.writeTestObject(self.uploadId, pn, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            var etag = res.headers.etag;

            self.commitUpload(self.uploadId, ['', etag, ''], function (err3) {
                if (!err3) {
                    t.fail('commit parts 0 and 2 have empty etags');
                    t.end();
                    return;
                }

                t.ok(verror.hasCauseWithName(err3,
                    'MultipartUploadInvalidArgumentError'));
                t.end();
            });
        });
    });
});


test('commit upload: incorrect part etag', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = 0;
        self.writeTestObject(self.uploadId, pn, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            self.commitUpload(self.uploadId, ['foobar'], function (err3) {
                if (!err3) {
                    t.fail('commit part 0 has incorrect etag');
                    t.end();
                    return;
                }

                t.ok(verror.hasCauseWithName(err3,
                    'MultipartUploadInvalidArgumentError'));
                t.end();
            });
        });
    });
});


test('commit upload: more than 10000 parts specified', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = 0;
        self.writeTestObject(self.uploadId, pn, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            var etag = res.headers.etag;
            var parts = [];
            for (var i = 0; i <= (helper.MAX_PART_NUM + 1); i++) {
                parts[i] = etag;
            }

            self.commitUpload(self.uploadId, parts, function (err3) {
                if (!err3) {
                    t.fail('commit specified > 10000 parts');
                    t.end();
                    return;
                }
                t.ok(verror.hasCauseWithName(err3,
                    'MultipartUploadInvalidArgumentError'));
                t.end();
            });
        });
    });
});


test('commit upload: non-uuid id', function (t) {
    var self = this;
    var bogus = 'foobar';
    var action = 'commit';

    var options = {
        headers: {
            'content-type': 'application/json',
            'expect': 'application/json'
        },
        path: '/' + this.client.user + '/uploads/f/' + bogus + '/' + action
    };

    self.client.signRequest({
        headers: options.headers
    },
    function (err) {
        if (ifErr(t, err, 'sign request')) {
            t.end();
            return;
        }

        // We have to use the jsonClient directly or we will blow an assertion
        // in node-manta, as the ID isn't a uuid.
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


test('commit upload: non-existent id', function (t) {
    var self = this;
    var bogus = uuid.v4();
    self.uploadId = bogus;
    self.commitUpload(bogus, [], function (err, upload) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err, 'ResourceNotFoundError'));
        t.end();
    });
});

test('commit upload: error on undefined parts array', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        self.commitUpload(self.uploadId, undefined, function (err2) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2,
                'MultipartUploadInvalidArgumentError'));
            t.end();
        });
    });
});
