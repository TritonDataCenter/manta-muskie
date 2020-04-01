/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var MemoryStream = require('stream').PassThrough;
var uuid = require('node-uuid');
var vasync = require('vasync');

if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');



///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;



///--- Helpers
function writeObject(client, key, opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }

    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';
    var size = Buffer.byteLength(text);

    var _opts = {
        headers: opts.headers,
        size: size
    };

    client.put(key, stream, _opts, cb);
    process.nextTick(stream.end.bind(stream, text));
}

function writeStreamingObject(client, key, opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }

    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';

    var _opts = {
        headers: opts.headers
    };

    process.nextTick(stream.end.bind(stream, text));
    client.put(key, stream, _opts, function (err, res) {
        if (err) {
            cb(err);
        } else if (res.statusCode != 204) {
            cb(new Error('unsuccessful object write'));
        } else {
            cb();
        }
    });
}

/*
 * Tests whether (or not) we are allowed to list the contents of a dir, given
 * these input arguments:
 *   - t: the test object - to be passed in from a wrapper function
 *   - isOperator: whether we want to perform the request as an operator or not
 *   - expectOk: whether we want the request to succeed or fail
 *   - path: the directory to list
 *   - params: an object containing the desired query parameters, of the form:
 *     {
 *        key1: "value1"
 *        key2: "value2"
 *        ...
 *     }
 *
 * This function does not verify that the request returns well-formed results.
 * See testListWithParams for that.
 *
 * Because some query parameters are not exposed through the node-manta client,
 * we use a lower-level JSON client to perform the request.
 */
function testParamsAllowed(t, isOperator, expectOk, path, params) {
    var queryParams = params || {};
    var client = helper.createJsonClient();

    var key;
    var user;
    if (isOperator) {
        key = helper.getOperatorPrivkey();
        user = helper.TEST_OPERATOR;
    } else {
        key = helper.getRegularPrivkey();
        user = process.env.MANTA_USER;
    }
    var keyId = helper.getKeyFingerprint(key);
    var signOpts = {
        key: key,
        keyId: keyId,
        user: user
    };

    // Perform the ls request, and check the response according to expectOk
    helper.signRequest(signOpts, function gotSignature(err, authz, date) {
        var opts = {
            headers: {
                authorization: authz,
                date: date
            },
            path: path,
            query: queryParams
        };
        client.get(opts, function (get_err, get_req, get_res) {
            if (expectOk) {
                t.ifError(get_err);
            } else {
                t.ok(get_err, 'expected error');
                t.equal(get_err.statusCode, 403, 'expected statuscode of 403');
                t.equal(get_err.restCode, 'QueryParameterForbidden');
            }
            t.end();
        });
    });
}

/*
 * Verifies that we get well-formed results back when listing the contents of a
 * dir with the given query parameters. Arguments:
 *   - t: the test object - to be passed in from a wrapper function
 *   - params: an object containing the desired query parameters, of the form:
 *     {
 *        key1: "value1"
 *        key2: "value2"
 *        ...
 *     }
 *
 * This function ensures that the request will be permitted by always performing
 * the request as an operator, and thus does not test access control. See
 * testParamsAllowed for that.
 *
 * Because some query parameters are not exposed through the node-manta client,
 * we use a lower-level JSON client to perform the request.
 */
function testListWithParams(t, params) {
    var self = this;
    var queryParams = params || {};
    var client = helper.createJsonClient();

    var key = helper.getOperatorPrivkey();
    var keyId = helper.getKeyFingerprint(key);
    var user = helper.TEST_OPERATOR;
    var signOpts = {
        key: key,
        keyId: keyId,
        user: user
    };

    // Generate some subdirectory names we can read back
    var subdirs = [];
    var count = 5;
    var i;
    for (i = 0; i < count; i++) {
        subdirs.push(self.dir + '/' + uuid.v4());
    }
    subdirs = subdirs.sort();


    // Make the subdirectories
    vasync.forEachParallel({
        func: mkdir,
        inputs: subdirs
    }, function subdirsCreated(mkdir_err, results) {
        t.ifError(mkdir_err);

        // Read the subdirectories back with the specified query params
        helper.signRequest(signOpts, function gotSig(sig_err, authz, date) {
            t.ifError(sig_err);
            var opts = {
                headers: {
                    authorization: authz,
                    date: date
                },
                path: self.dir,
                query: queryParams
            };
            client.get(opts, function (get_err, get_req, get_res) {
                t.ifError(get_err);

                // Parse the response body into a list of directory names
                var jsonStrings = get_res.body.split('\n').filter(isNotEmpty);
                var names = [];
                jsonStrings.forEach(function appendName(s) {
                    t.doesNotThrow(parseAndAppend.bind(null, s, names));
                });
                names.sort();

                // Verify that we got back all of the directories we created
                t.deepEqual(subdirs, names.map(prependDir));
                t.end();
            });
        });
    });

    // helper functions

    function mkdir(path, cb) {
        self.operatorClient.mkdir(path, function madeDir(err, res) {
            t.ifError(err);
            t.ok(res);
            t.checkResponse(res, 204);
            cb(err);
        });
    }

    function isNotEmpty(str) {
        return (str !== '');
    }

    function parseAndAppend(str, list) {
        list.push(JSON.parse(str).name);
    }

    function prependDir(name) {
        return (self.dir + '/' + name);
    }
}


///--- Tests

before(function (cb) {
    this.client = helper.createClient();
    this.operatorClient = helper.createOperatorClient();
    this.top = '/' + this.client.user;
    this.root = this.top + '/stor';
    this.dir = this.root + '/' + uuid.v4();
    this.key = this.dir + '/' + uuid.v4();
    this.client.mkdir(this.dir, cb);
});


after(function (cb) {
    this.client.rmr(this.dir, cb.bind(null, null));
});


test('mkdir', function (t) {
    this.client.mkdir(this.key, function (err, res) {
        t.ifError(err);
        t.ok(res);
        t.checkResponse(res, 204);
        t.end();
    });
});


test('mkdir overwrite', function (t) {
    var self = this;
    this.client.mkdir(this.key, function (err, res) {
        t.ifError(err);
        t.ok(res);
        t.checkResponse(res, 204);
        self.client.mkdir(self.key, function (err2, res2) {
            t.ifError(err2);
            t.ok(res2);
            t.checkResponse(res2, 204);
            t.end();
        });
    });
});


test('mkdir, chattr: m- headers', function (t) {
    var k = this.key;
    var opts = {
        headers: {
            'm-foo': 'bar',
            'm-bar': 'baz'
        }
    };
    var self = this;

    this.client.mkdir(k, function (err) {
        t.ifError(err);

        self.client.chattr(k, opts, function (err2) {
            t.ifError(err2);

            if (err2) {
                t.end();
                return;
            }

            self.client.info(k, function (err3, info) {
                t.ifError(err3);
                t.ok(info);
                if (info) {
                    var h = info.headers || {};
                    t.equal(h['m-foo'], 'bar');
                    t.equal(h['m-bar'], 'baz');
                    t.equal(h['content-type'],
                            'application/x-json-stream; type=directory');
                }
                t.end();
            });
        });
    });
});



test('mkdir, chattr: content-type (ignore)', function (t) {
    var k = this.key;
    var opts = {
        headers: {
            'content-type': 'jpg'
        }
    };
    var self = this;

    this.client.mkdir(k, function (err) {
        t.ifError(err);

        self.client.chattr(k, opts, function (err2) {
            t.ifError(err2);

            self.client.info(k, function (err3, info) {
                t.ifError(err3);
                t.equal(info.extension, 'directory');
                t.end();
            });
        });
    });
});



test('put then overwrite w/directory', function (t) {
    var self = this;

    writeObject(this.client, this.key, function (put_err) {
        t.ifError(put_err);
        self.client.mkdir(self.key, function (err, res) {
            t.ifError(err);
            t.checkResponse(res, 204);
            t.end();
        });
    });
});


test('mkdir top', function (t) {
    this.client.mkdir(this.top, function (err, res) {
        t.ok(err);
        t.ok(res);
        t.equal(err.name, 'OperationNotAllowedOnRootDirectoryError');
        t.checkResponse(res, 400);
        t.end();
    });
});


test('ls returns content-type for non-streaming objects', function (t) {
    var self = this;

    writeObject(self.client, self.key, function (put_err) {
        t.ifError(put_err);
        self.client.ls(self.dir, function (err, res) {
            t.ifError(err);
            t.ok(res);

            var objs = [];

            res.on('object', function (obj) {
                t.ok(obj, 'fail, no obj!');
                objs.push(obj);
            });

            res.once('error', function (err2) {
                t.ifError(err2);
                t.end();
            });

            res.once('end', function (http_res) {
                t.ok(http_res);
                t.checkResponse(http_res, 200);
                t.equal(objs.length, 1);
                t.ok(objs[0].contentType);
                t.equal(objs[0].contentType, 'application/octet-stream');
                t.end();
            });
        });
    });
});


test('ls returns content-type for streaming objects', function (t) {
    var self = this;

    writeStreamingObject(self.client, self.key, function (put_err) {
        t.ifError(put_err);
        self.client.ls(self.dir, function (err, res) {
            t.ifError(err);
            t.ok(res);

            var objs = [];

            res.on('object', function (obj) {
                t.ok(obj, 'fail, no obj!');
                objs.push(obj);
            });

            res.once('error', function (err2) {
                t.ifError(err2);
                t.end();
            });

            res.once('end', function (http_res) {
                t.ok(http_res);
                t.checkResponse(http_res, 200);
                t.equal(objs.length, 1);
                t.ok(objs[0].contentType);
                t.equal(objs[0].contentType, 'application/octet-stream');
                t.end();
            });
        });
    });
});


test('ls returns contentMD5 for objects', function (t) {
    var self = this;

    writeObject(self.client, self.key, function (put_err) {
        t.ifError(put_err);
        self.client.ls(self.dir, function (err, res) {
            t.ifError(err);
            t.ok(res);

            var objs = [];

            res.on('object', function (obj) {
                t.ok(obj, 'fail, no obj!');
                objs.push(obj);
            });

            res.once('error', function (err2) {
                t.ifError(err2);
                t.end();
            });

            res.once('end', function (http_res) {
                t.ok(http_res);
                t.checkResponse(http_res, 200);
                t.equal(objs.length, 1);
                t.ok(objs[0].contentMD5);
                self.client.info(self.key, {}, function (err2, info) {
                    t.ifError(err2);
                    t.ok(info);
                    if (info) {
                        t.ok(info.md5);
                        t.equal(objs[0].contentMD5, info.md5);
                    }
                    t.end();
                });
            });
        });
    });
});


test('ls top', function (t) {
    this.client.ls(this.top, function (err, res) {
        t.ifError(err);
        t.ok(res);

        var objs = [];
        var dirs = [];

        res.on('object', function (obj) {
            t.ok(obj, 'fail, no obj!');
            objs.push(obj);
        });

        res.on('directory', function (dir) {
            t.ok(dir, 'fail, no dir!');
            dirs.push(dir);
        });

        res.once('error', function (err2) {
            t.ifError(err2);
            t.end();
        });

        res.once('end', function (http_res) {
            t.ok(http_res);
            t.checkResponse(http_res, 200);
            t.equal(0, objs.length);

            var names = dirs.map(function (d) {
                return (d.name);
            }).filter(function (d) {
                if (d === 'uploads' || d === 'jobs') {
                    return (false);
                }
                return (true);
            }).sort();

            t.deepEqual(names, ['public', 'reports', 'stor']);
            t.end();
        });
    });
});


test('ls top with marker', function (t) {
    this.client.ls(this.top, { marker: 'public'}, function (err, res) {
        t.ifError(err);
        t.ok(res);

        var objs = [];
        var dirs = [];

        res.on('object', function (obj) {
            t.ok(obj, 'fail, no obj!');
            objs.push(obj);
        });

        res.on('directory', function (dir) {
            t.ok(dir, 'fail, no dir!');
            dirs.push(dir);
        });

        res.once('error', function (err2) {
            t.ifError(err2);
            t.end();
        });

        res.once('end', function (http_res) {
            t.ok(http_res);
            t.checkResponse(http_res, 200);
            t.equal(0, objs.length);

            var names = dirs.map(function (d) {
                return (d.name);
            }).filter(function (d) {
                if (d === 'uploads' || d === 'jobs') {
                    return (false);
                }
                return (true);
            }).sort();

            t.deepEqual(names, ['reports', 'stor']);
            t.end();
        });
    });
});


test('rmdir top', function (t) {
    this.client.unlink(this.top, function (err, res) {
        t.ok(err);
        t.ok(res);
        t.equal(err.name, 'OperationNotAllowedOnRootDirectoryError');
        t.checkResponse(res, 400);
        t.end();
    });
});

test('mkdir root', function (t) {
    this.client.mkdir(this.root, function (err, res) {
        t.ifError(err);
        t.ok(res);
        t.checkResponse(res, 204);
        t.end();
    });
});


test('mkdir no parent',  function (t) {
    var key = this.root + '/' + uuid.v4() + '/' + uuid.v4();
    this.client.mkdir(key, function (err, res) {
        t.ok(err);
        t.ok(res);
        t.equal(err.name, 'DirectoryDoesNotExistError');
        t.checkResponse(res, 404);
        t.end();
    });
});


test('put under non-directory', function (t) {
    var self = this;

    writeObject(this.client, this.key, function (put_err) {
        t.ifError(put_err);
        var key = self.key + '/' + uuid.v4();
        self.client.mkdir(key, function (err, res) {
            t.ok(err);
            t.ok(res);
            t.equal(err.name, 'ParentNotDirectoryError');
            t.checkResponse(res, 400);
            t.end();
        });
    });
});


test('ls empty', function (t) {
    this.client.ls(this.dir, function (err, res) {
        t.ifError(err);
        t.ok(res);

        res.on('object', function (obj) {
            t.ok(!obj, 'fail!');
        });
        res.on('directory', function (dir) {
            t.ok(!dir, 'fail!');
        });
        res.once('error', function (err2) {
            t.ifError(err2);
            t.end();
        });
        res.once('end', function (http_res) {
            t.ok(http_res);
            t.checkResponse(http_res, 200);
            t.end();
        });
    });
});


test('ls with obj and dir', function (t) {
    var dir = false;
    var key = this.dir + '/' + uuid.v4();
    var obj = false;
    var self = this;

    vasync.pipeline({
        funcs: [
            function (_, cb) {
                writeObject(self.client, key, cb);
            },
            function (_, cb) {
                self.client.mkdir(self.key, cb);
            }
        ]
    }, function (err) {
        t.ifError(err);
        self.client.ls(self.dir, function (err2, res) {
            t.ifError(err2);
            t.ok(res);

            res.on('object', function (o) {
                t.ok(o);
                t.equal(o.type, 'object');
                t.ok(o.etag);
                t.ok(o.mtime);
                t.equal(o.name, key.split('/').pop());
                obj = true;
            });
            res.on('directory', function (d) {
                t.ok(d);
                t.equal(d.type, 'directory');
                t.ok(d.mtime);
                t.equal(d.name, self.key.split('/').pop());
                dir = true;
            });
            res.once('error', function (err3) {
                t.ifError(err3);
                t.end();
            });
            res.once('end', function (http_res) {
                t.ok(http_res);
                t.checkResponse(http_res, 200);
                t.ok(dir);
                t.ok(obj);
                t.end();
            });
        });
    });
});


test('ls escapes path', function (t) {
    var self = this;
    var key = this.dir + '/*needs=(escaping)';
    this.client.mkdir(key, function (err, res) {
        t.ifError(err);
        t.ok(res);
        t.checkResponse(res, 204);
        self.client.ls(key, function (err2, res2) {
            t.ifError(err2);
            t.ok(res2);

            res2.once('error', function (err3) {
                t.ifError(err3);
                t.end();
            });
            res2.once('end', function (http_res) {
                t.ok(http_res);
                t.checkResponse(http_res, 200);
                t.end();
            });
        });
    });
});


test('ls escapes marker', function (t) {
    var self = this;
    var key = 'needs)*(=escaping';
    var files = ['aaa', key, 'zzz'];
    vasync.pipeline({
        funcs: files.map(function (file) {
            return function (_, cb) {
                var path = self.dir + '/' + file;
                writeObject(self.client, path, cb);
            };
        })
    }, function (verr) {
        t.ifError(verr);
        self.client.ls(self.dir, { marker: key }, function (err, res) {
            t.ifError(err);
            t.ok(res);

            var objs = [];

            res.on('object', function (obj) {
                t.ok(obj, 'fail, no obj!');
                objs.push(obj);
            });

            res.on('directory', function (dir) {
                t.ok(!dir, 'fail, unexpected dir!');
            });

            res.once('error', function (err2) {
                t.ifError(err2);
                t.end();
            });

            res.once('end', function (http_res) {
                t.ok(http_res);
                t.checkResponse(http_res, 200);
                t.equal(2, objs.length);
                var names = objs.map(function (d) {
                    return (d.name);
                }).sort();
                t.deepEqual([key, 'zzz'], names);
                t.end();
            });
        });
    });
});


test('ls 404', function (t) {
    this.client.ls(this.dir + '/' + uuid.v4(), function (err, res) {
        t.ok(err);
        t.equal(err.name, 'NotFoundError');
        t.end();
        // t.checkResponse(res, 404);
        // t.ifError(res);

        // res.once('error', function (err2, http_res) {
        // });

        // res.once('end', function () {
        //     t.ok(false, 'should have errored');
        // });
    });
});


test('operator can ls with no sort', function (t) {
    testParamsAllowed(t, true, true, this.root, {
        sort: 'none'
    });
});


test('operator can ls with no owner check', function (t) {
    testParamsAllowed(t, true, true, this.root, {
        skip_owner_check: 'true'
    });
});


test('operator can ls with sort and no owner check', function (t) {
    testParamsAllowed(t, true, true, this.root, {
        sort: 'none',
        skip_owner_check: 'true'
    });
});


test('regular user cannot ls with no sort', function (t) {
    testParamsAllowed(t, false, false, this.root, {
        sort: 'none'
    });
});


test('regular user cannot ls with no owner check', function (t) {
    testParamsAllowed(t, false, false, this.root, {
        skip_owner_check: 'true'
    });
});


test('regular user cannot ls with no sort and no owner check', function (t) {
    testParamsAllowed(t, false, false, this.root, {
        sort: 'none',
        skip_owner_check: 'true'
    });
});

test('ls with no sort returns accurate list of results', function (t) {
    testListWithParams.bind(this)(t, {
        sort: 'none'
    });
});

test('ls with no owner check returns accurate list of results', function (t) {
    testListWithParams.bind(this)(t, {
        skip_owner_check: 'true'
    });
});

test('ls with no sort and no owner check returns accurate list of results',
    function (t) {
    testListWithParams.bind(this)(t, {
        sort: 'none',
        skip_owner_check: 'true'
    });
});

test('rmdir', function (t) {
    this.client.unlink(this.dir, function (err, res) {
        t.ifError(err);
        t.checkResponse(res, 204);
        t.end();
    });
});


test('rmdir 404', function (t) {
    this.client.unlink(this.root + '/' + uuid.v4(), function (err, res) {
        t.ok(err);
        t.equal(err.name, 'ResourceNotFoundError');
        t.checkResponse(res, 404);
        t.end();
    });
});


test('rmdir 404', function (t) {
    this.client.unlink(this.root + '/' + uuid.v4(), function (err, res) {
        t.ok(err);
        t.equal(err.name, 'ResourceNotFoundError');
        t.checkResponse(res, 404);
        t.end();
    });
});


test('rmdir root', function (t) {
    this.client.unlink(this.root, function (err, res) {
        t.ok(err);
        t.ok(res);
        t.equal(err.name, 'OperationNotAllowedOnRootDirectoryError');
        t.checkResponse(res, 400);
        t.end();
    });
});


test('rmdir not empty', function (t) {
    var self = this;
    writeObject(this.client, this.key, function (put_err) {
        t.ifError(put_err);
        self.client.unlink(self.dir, function (err, res) {
            t.ok(err);
            t.ok(res);
            t.equal(err.name, 'DirectoryNotEmptyError');
            t.checkResponse(res, 400);
            t.end();
        });
    });
});

test('mkdir with Content-Disposition ignored', function (t) {
    var cd = 'attachment; filename="my-file.txt"';
    var opts = {
        headers: {
            'content-disposition': cd
        }
    };

    var self = this;

    self.client.mkdir(self.key, opts, function (err, res) {
        t.ifError(err);
        t.ok(res);
        t.checkResponse(res, 204);
        t.end();

        self.client.info(self.key, function (err2, info) {
                t.ifError(err2);
                t.ok(info);
                if (info) {
                    t.ok(!('content-disposition' in info.headers),
                         'content-dispostion should not be in headers');
                }
                t.end();
            });
    });
});

test('mkdir with bad Content-Disposition ignored', function (t) {
    var cd = 'attachment;"';
    var opts = {
        headers: {
            'content-disposition': cd
        }
    };

    var self = this;

    self.client.mkdir(self.key, opts, function (err, res) {
        t.ifError(err);
        t.ok(res);
        t.checkResponse(res, 204);
        t.end();

        self.client.info(self.key, function (err2, info) {
            t.ifError(err2);
            t.ok(info);
            t.ok(!('content-disposition' in info.headers),
                 'content-dispostion should not be in headers');
            t.end();
        });
    });
});

test('mkdir, chattr: content-disposition (ignore)', function (t) {
    var k = this.key;
    var cd = 'attachment"';
    var opts = {
        headers: {
            'content-disposition': cd
        }
    };
    var self = this;

    this.client.mkdir(k, function (err) {
        t.ifError(err);

        self.client.chattr(k, opts, function (err2) {
            t.ifError(err2);

            self.client.info(k, function (err3, info) {
                t.ifError(err3);
                 t.ok(info);
                t.ok(!('content-disposition' in info.headers),
                     'content-dispostion should not be in headers');
                t.equal(info.extension, 'directory');
                t.end();
            });
        });
    });
});

test('mkdir, chattr: bad content-disposition (ignore)', function (t) {
    var k = this.key;
    var cd = 'attachment;"';
    var opts = {
        headers: {
            'content-disposition': cd
        }
    };
    var self = this;

    this.client.mkdir(k, function (err) {
        t.ifError(err);

        self.client.chattr(k, opts, function (err2) {
            t.ifError(err2);

            self.client.info(k, function (err3, info) {
                t.ifError(err3);
                 t.ok(info);
                t.ok(!('content-disposition' in info.headers),
                     'content-dispostion should not be in headers');
                t.equal(info.extension, 'directory');
                t.end();
            });
        });
    });
});

test('ls returns content-disposition for non-streaming objects', function (t) {
    var self = this;
    var cd = 'attachment; filename="my-file.txt"';
    var opts = {
        headers: {
            'content-disposition': cd
        }
    };

    writeObject(self.client, self.key, opts, function (put_err) {
        t.ifError(put_err);
        self.client.ls(self.dir, function (err, res) {
            t.ifError(err);
            t.ok(res);

            var objs = [];

            res.on('object', function (obj) {
                t.ok(obj, 'fail, no obj!');
                objs.push(obj);
            });

            res.once('error', function (err2) {
                t.ifError(err2);
                t.end();
            });

            res.once('end', function (http_res) {
                t.ok(http_res);
                t.checkResponse(http_res, 200);
                t.equal(objs.length, 1);
                t.ok(objs[0].contentDisposition);
                t.equal(objs[0].contentDisposition, cd);
                t.end();
            });
        });
    });
});

test('ls returns content-disposition for streaming objects', function (t) {
    var self = this;
    var cd = 'attachment; filename="my-file.txt"';
    var opts = {
        headers: {
            'content-disposition': cd
        }
    };

    writeStreamingObject(self.client, self.key, opts, function (put_err) {
        t.ifError(put_err);
        self.client.ls(self.dir, function (err, res) {
            t.ifError(err);
            t.ok(res);

            var objs = [];

            res.on('object', function (obj) {
                t.ok(obj, 'fail, no obj!');
                objs.push(obj);
            });

            res.once('error', function (err2) {
                t.ifError(err2);
                t.end();
            });

            res.once('end', function (http_res) {
                t.ok(http_res);
                t.checkResponse(http_res, 200);
                t.equal(objs.length, 1);
                t.ok(objs[0].contentDisposition);
                t.equal(objs[0].contentDisposition, cd);
                t.end();
            });
        });
    });
});

test('ls returns no content-disposition when not set', function (t) {
    var self = this;

    writeObject(self.client, self.key, function (put_err) {
        t.ifError(put_err);
        self.client.ls(self.dir, function (err, res) {
            t.ifError(err);
            t.ok(res);

            var objs = [];

            res.on('object', function (obj) {
                t.ok(obj, 'fail, no obj!');
                objs.push(obj);
            });

            res.once('error', function (err2) {
                t.ifError(err2);
                t.end();
            });

            res.once('end', function (http_res) {
                t.ok(http_res, 'http response exists');
                t.checkResponse(http_res, 200);
                t.equal(objs.length, 1, 'one object in list');
                t.ok(!(objs[0].contentDisposition),
                     'no content disposition field');
                t.end();
            });
        });
    });
});
