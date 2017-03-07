/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
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

function writeObject(client, key, cb) {
    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';
    var size = Buffer.byteLength(text);

    client.put(key, stream, {size: size}, cb);
    process.nextTick(stream.end.bind(stream, text));
}



///--- Tests

before(function (cb) {
    this.client = helper.createClient();
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
            t.equal(5, dirs.length);
            var names = dirs.map(function (d) {
                return (d.name);
            }).sort();
            t.deepEqual(['jobs', 'public', 'reports', 'stor', 'uploads'],
                        names);
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
            t.equal(3, dirs.length);
            var names = dirs.map(function (d) {
                return (d.name);
            }).sort();
            t.deepEqual(['reports', 'stor', 'uploads'], names);
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
