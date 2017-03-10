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

// Commit

// TODO: commit > 1 part
// TODO: already committed > 1 part
// TODO: max upload size exceeded
// TODO: different md5 than specified on create


test('commit upload: zero parts', function (t) {
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

        self.client.commitUpload(o.id, [], opts, function (err3) {
            if (ifErr(t, err3, 'committed upload')) {
                t.end();
                return;
            }

            self.client.getUpload(o.id, opts, function (err4, upload) {
                if (ifErr(t, err4, 'created upload')) {
                    t.end();
                    return;
                }

                sanityCheckUpload(t, o, upload);
                t.deepEqual(upload.headers, {});
                t.equal(upload.state, 'finalizing');
                t.equal(upload.type, 'commit');
                t.equal(upload.partsMD5, computePartsMD5([]));
                t.end();
            });
        });
    });
});


test('commit upload: one part', function (t) {
    var self = this;
    var a = self.client.user;

    createUpload(self, a, self.path, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var opts = createPartOptions(a, helper.TEXT);

        var pn = 0;
        writeObject(self.client, o.id, pn, opts, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            var etag = res.headers.etag;
            opts = {
                account: a
            };

            self.client.commitUpload(o.id, [etag], opts, function (err3) {
                if (ifErr(t, err3, 'committed upload')) {
                    t.end();
                    return;
                }

                self.client.getUpload(o.id, opts, function (err4, upload) {
                    if (ifErr(t, err4, 'created upload')) {
                        t.end();
                        return;
                    }

                    sanityCheckUpload(t, o, upload);
                    t.deepEqual(upload.headers, {});
                    t.equal(upload.state, 'finalizing');
                    t.equal(upload.type, 'commit');
                    t.equal(upload.partsMD5, computePartsMD5([etag]));
                    t.end();
                });
            });
        });
    });
});


test('commit upload: already commited, same set of parts', function (t) {
    var self = this;
    var a = self.client.user;

    createUpload(self, a, self.path, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var pn = 0;
        var opts = createPartOptions(a, helper.TEXT);

        writeObject(self.client, o.id, pn, opts, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            var etag = res.headers.etag;
            opts = {
                account: a
            };

            self.client.commitUpload(o.id, [etag], opts, function (err3) {
                if (ifErr(t, err3, 'committed upload')) {
                    t.end();
                    return;
                }

                self.client.commitUpload(o.id, [etag], opts, function (err4) {
                    if (ifErr(t, err4, 'committed upload')) {
                        t.end();
                        return;
                    }

                    self.client.getUpload(o.id, opts, function (err5, upload) {
                        if (ifErr(t, err5, 'got upload')) {
                            t.end();
                            return;
                        }

                        sanityCheckUpload(t, o, upload);
                        t.deepEqual(upload.headers, {});
                        t.equal(upload.state, 'finalizing');
                        t.equal(upload.type, 'commit');
                        t.equal(upload.partsMD5, computePartsMD5([etag]));
                        t.end();
                    });
                });
            });
        });
    });
});


// Commit: invalid upload (not related to the JSON API inputs)

test('commit upload: already aborted', function (t) {
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
            if (ifErr(t, err2, 'created upload')) {
                t.end();
                return;
            }

            self.client.commitUpload(o.id, [], opts, function (err3) {
                if (!err3) {
                    t.fail('upload already aborted');
                    t.end();
                    return;
                }

                t.ok(verror.hasCauseWithName(err3,
                    'MultipartUploadAbortedError'));
                t.end();
            });
        });
    });
});


test('commit upload: already committed, different set of parts', function (t) {
    var self = this;
    var a = self.client.user;

    createUpload(self, a, self.path, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var pn = 0;
        var opts = createPartOptions(a, helper.TEXT);

        writeObject(self.client, o.id, pn, opts, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            var etag = res.headers.etag;
            opts = {
                account: a
            };

            self.client.commitUpload(o.id, [etag, etag], opts, function (err3) {
                if (!err3) {
                    t.fail('upload already committed with different part set');
                    t.end();
                    return;
                }

                t.ok(verror.hasCauseWithName(err3,
                    'MultipartUploadMissingPartError'));
                t.end();
            });
        });
    });
});


test('commit upload: object size does not match create header (0 parts)',
function (t) {
    var self = this;
    var a = self.client.user;

    var h = {
        'content-length': helper.TEXT.length
    };

    createUpload(self, a, self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var opts = {
           account: a
        };

        self.client.commitUpload(o.id, [], opts, function (err2) {
            if (!err2) {
                t.fail('object size mismatch');
                t.end();
                return;
            }

            t.ok(verror.hasCauseWithName(err2,
                'MultipartUploadContentLengthError'));
            t.end();
        });
    });

});


test('commit upload: object size does not match create header (1 part)',
function (t) {
    var self = this;
    var a = self.client.user;

    var h = {
        'content-length': helper.TEXT.length + 1
    };

    createUpload(self, a, self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var pn = 0;
        var opts = createPartOptions(a, helper.TEXT);

        writeObject(self.client, o.id, pn, opts, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            var etag = res.headers.etag;
            opts = {
                account: a
            };

            self.client.commitUpload(o.id, [etag], opts, function (err3) {
                if (!err3) {
                    t.fail('object size mismatch');
                    t.end();
                    return;
                }

                t.ok(verror.hasCauseWithName(err3,
                    'MultipartUploadContentLengthError'));
                t.end();
            });
        });
    });
});


test('commit upload: non-final part less than min part size', function (t) {
    var self = this;
    var a = self.client.user;

    createUpload(self, a, self.path, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var opts = createPartOptions(a, helper.TEXT);
        var etags = [];

        vasync.forEachParallel({
            func: function uploadText(pn, cb) {
                writeObject(self.client, o.id, pn, opts, function (errw, res) {
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

            opts = {
                account: a
            };

            self.client.commitUpload(o.id, etags, opts, function (err2) {
                if (!err2) {
                    t.fail('non-final part has less than minimum size');
                    t.end();
                    return;
                }

                t.ok(verror.hasCauseWithName(err2,
                    'MultipartUploadPartSizeError'));
                t.end();
            });
        });
    });
});


// Commit: invalid object path specifed on create

test('commit upload: path is top-level directory', function (t) {
    var self = this;
    var a = self.client.user;
    var p = '/' + a + '/stor';

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var opts = {
           account: a
        };

        self.client.commitUpload(o.id, [], opts, function (err3) {
            if (!err3) {
                t.fail('invalid object path (top-level directory)');
                t.end();
                return;
            }

            t.ok(verror.hasCauseWithName(err3,
                'OperationNotAllowedOnDirectoryError'));
             t.end();
        });
    });
});


test('commit upload: parent dir does not exist (parent is top-level dir)',
function (t) {
    var self = this;
    var a = self.client.user;
    var p = '/' + a + '/' + uuid.v4() + '/foo.txt';

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var opts = {
           account: a
        };

        self.client.commitUpload(o.id, [], opts, function (err3) {
            if (!err3) {
                t.fail('invalid object path (parent is top-level directory)');
                t.end();
                return;
            }

            t.ok(verror.hasCauseWithName(err3, 'DirectoryDoesNotExistError'));
             t.end();
        });
    });
});


test('commit upload: parent dir does not exist (parent is not a top-level dir)',
function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir + '/foobar/foo.txt';

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var opts = {
           account: a
        };

        self.client.commitUpload(o.id, [], opts, function (err3) {
            if (!err3) {
                t.fail('invalid object path (parent does not exist)');
                t.end();
                return;
            }

            t.ok(verror.hasCauseWithName(err3,
                'DirectoryDoesNotExistError'));
             t.end();
        });
    });
});


test('commit upload: object path under another account', function (t) {
    var self = this;
    var a = self.client.user;
    var p = '/poseidon/stor/foo.txt';

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };

        self.client.commitUpload(o.id, [], opts, function (err2) {
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


test('commit upload: object path under a nonexistent account', function (t) {
    var self = this;
    var a = self.client.user;
    var bogus = uuid.v4();
    var p = '/' + bogus + '/foo.txt';

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };

        self.client.commitUpload(o.id, [], opts, function (err2) {
            if (!err2) {
                t.fail('upload created under a different account');
                t.end();
                return;
            }

            t.ok(verror.hasCauseWithName(err2,
                'AccountDoesNotExistError'));
            t.end();
        });
    });
});




// Commit: bad inputs to API

test('commit upload: empty part etag', function (t) {
    var self = this;
    var a = self.client.user;

    createUpload(self, a, self.path, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var pn = 0;
        var opts = createPartOptions(a, helper.TEXT);

        writeObject(self.client, o.id, pn, opts, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            opts = {
                account: a
            };

            self.client.commitUpload(o.id, [''], opts, function (err3) {
                if (!err3) {
                    t.fail('commit part 0 has an empty etag');
                    t.end();
                    return;
                }

                t.ok(verror.hasCauseWithName(err3,
                    'MultipartUploadMissingPartError'));
                t.end();
            });
        });
    });
});


test('commit upload: incorrect part etag', function (t) {
    var self = this;
    var a = self.client.user;

    createUpload(self, a, self.path, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var pn = 0;
        var opts = createPartOptions(a, helper.TEXT);

        writeObject(self.client, o.id, pn, opts, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            opts = {
                account: a
            };

            self.client.commitUpload(o.id, ['foobar'], opts, function (err3) {
                if (!err3) {
                    t.fail('commit part 0 has incorrect etag');
                    t.end();
                    return;
                }

                t.ok(verror.hasCauseWithName(err3,
                    'MultipartUploadPartEtagError'));
                t.end();
            });
        });
    });
});


test('commit upload: more than 10000 parts specified', function (t) {
    var self = this;
    var a = self.client.user;

    createUpload(self, a, self.path, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var pn = 0;
        var opts = createPartOptions(a, helper.TEXT);

        writeObject(self.client, o.id, pn, opts, function (err2, res) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);

            var etag = res.headers.etag;
            opts = {
                account: a
            };

            var parts = [];
            for (var i = 0; i <= (helper.MAX_PART_NUM + 1); i++) {
                parts[i] = etag;
            }

            self.client.commitUpload(o.id, parts, opts, function (err3) {
                if (!err3) {
                    t.fail('commit specified > 10000 parts');
                    t.end();
                    return;
                }
                t.ok(verror.hasCauseWithName(err3,
                    'MultipartUploadPartLimitError'));
                t.end();
            });
        });
    });
});


test('commit upload: non-uuid id', function (t) {
    var self = this;
    var opts = {
         account: this.client.user
    };

    var bogus = 'foobar';
    self.client.commitUpload(bogus, [], opts, function (err, upload) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err, 'ResourceNotFoundError'), err);
        t.end();
    });
});


test('commit upload: non-existent id', function (t) {
    var self = this;
    var opts = {
         account: this.client.user
    };

    var bogus = uuid.v4();
    self.client.commitUpload(bogus, [], opts, function (err, upload) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err, 'ResourceNotFoundError'));
        t.end();
    });
});
