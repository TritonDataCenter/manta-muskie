/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var assert = require('assert-plus');
var MemoryStream = require('stream').PassThrough;
var test = require('tap').test;
var uuidv4 = require('uuid/v4');
var vasync = require('vasync');

var helper = require('../helper');



///--- Globals

var assertMantaRes = helper.assertMantaRes;


///--- Helpers

function writeObject(client_, key, opts, cb) {
    assert.string(key, 'key');

    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }

    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';
    var size = Buffer.byteLength(text);

    var putOpts = {
        headers: opts.headers,
        size: size
    };

    client_.put(key, stream, putOpts, cb);
    process.nextTick(stream.end.bind(stream, text));
}

function writeStreamingObject(client_, key, opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }

    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';

    var putOpts = {
        headers: opts.headers
    };

    process.nextTick(stream.end.bind(stream, text));
    client_.put(key, stream, putOpts, function (err, res) {
        if (err) {
            cb(err);
        } else if (res.statusCode != 204) {
            cb(new Error('unsuccessful object write'));
        } else {
            cb();
        }
    });
}


///--- Tests

var client;
var testAccount;
var testDir;

test('setup: test accounts', function (t) {
    helper.ensureTestAccounts(t, function (err, accounts) {
        t.ifError(err, 'no error loading/creating test accounts');
        testAccount = accounts.regular;
        t.ok(testAccount, 'have regular test account: ' +
            testAccount.login);
        t.end();
    });
});

test('setup: test dir', function (t) {
    client = helper.mantaClientFromAccountInfo(testAccount);
    testDir = '/' + testAccount.login +
        '/stor/test-dir-dir-' + uuidv4().split('-')[0];

    client.mkdir(testDir, function (err) {
        t.ifError(err, 'no error making testDir:' + testDir);
        t.end();
    });
});


test('mkdir', function (t) {
    var key = testDir + '/mkdir';
    client.mkdir(key, function (err, res) {
        t.ifError(err);
        assertMantaRes(t, res, 204);
        t.end();
    });
});

test('mkdir overwrite', function (t) {
    var key = testDir + '/mkdir-overwrite';
    client.mkdir(key, function (err) {
        t.ifError(err);
        client.mkdir(key, function (err2, res) {
            t.ifError(err2);
            assertMantaRes(t, res, 204);
            t.end();
        });
    });
});

test('mkdir, chattr: m- headers', function (t) {
    var key = testDir + '/mkdir-chattr-m-headers';
    var opts = {
        headers: {
            'm-foo': 'bar',
            'm-bar': 'baz'
        }
    };

    client.mkdir(key, function (err) {
        t.ifError(err);

        client.chattr(key, opts, function (chattrErr) {
            t.ifError(chattrErr);

            if (chattrErr) {
                t.end();
                return;
            }

            client.info(key, function (infoErr, info) {
                t.ifError(infoErr);
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

test('mkdir, chattr: given content-type ignored', function (t) {
    var key = testDir + '/mkdir-chattr-ignored-content-type';
    var opts = {
        headers: {
            'content-type': 'jpg'
        }
    };

    client.mkdir(key, function (err) {
        t.ifError(err);

        client.chattr(key, opts, function (err2) {
            t.ifError(err2);

            client.info(key, function (err3, info) {
                t.ifError(err3);
                t.equal(info.extension, 'directory');
                t.end();
            });
        });
    });
});


test('put then overwrite w/directory', function (t) {
    var key = testDir + '/put-object-then-mkdir-overwrite';
    writeObject(client, key, function (putErr) {
        t.ifError(putErr);
        client.mkdir(key, function (err, res) {
            t.ifError(err);
            assertMantaRes(t, res, 204);
            t.end();
        });
    });
});


test('mkdir top /:login dir', function (t) {
    var top = '/' + client.user;

    client.mkdir(top, function (err, res) {
        t.ok(err);
        assertMantaRes(t, res, 400);
        t.equal(err.name, 'OperationNotAllowedOnRootDirectoryError');
        t.end();
    });
});


test('ls returns content-type for non-streaming objects', function (t) {
    var key = testDir + '/ls-returns-content-type-for-non-streaming-objs';

    writeObject(client, key, function (putErr) {
        t.ifError(putErr);
        client.ls(testDir, function (err, res) {
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

            res.once('end', function (httpRes) {
                assertMantaRes(t, httpRes, 200);
                t.equal(objs.length, 1);
                t.ok(objs[0].contentType);
                t.equal(objs[0].contentType, 'application/octet-stream');
                t.end();
            });
        });
    });
});


test('ls returns content-type for streaming objects', function (t) {
    var subdir = testDir + '/ls-returns-content-type-for-streaming-objs';
    var key = subdir + '/the-obj';

    client.mkdir(subdir, function (mkdirErr) {
        writeStreamingObject(client, key, function (putErr) {
            t.ifError(putErr);
            client.ls(subdir, function (err, res) {
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

                res.once('end', function (httpRes) {
                    t.ok(httpRes);
                    assertMantaRes(t, httpRes, 200);
                    t.equal(objs.length, 1);
                    t.ok(objs[0].contentType);
                    t.equal(objs[0].contentType, 'application/octet-stream');
                    t.end();
                });
            });
        });
    });
});


test('ls returns contentMD5 for objects', function (t) {
    var key = testDir + '/ls-returns-content-md5-for-objs';

    writeObject(client, key, function (putErr) {
        t.ifError(putErr);
        client.ls(testDir, function (err, res) {
            t.ifError(err);
            t.ok(res);

            var objs = [];

            res.on('object', function (obj) {
                objs.push(obj);
            });

            res.once('error', function (err2) {
                t.ifError(err2);
                t.end();
            });

            res.once('end', function (httpRes) {
                assertMantaRes(t, httpRes, 200);
                t.ok(objs.length >= 1, 'have at least one object on testDir');
                t.ok(objs[0].contentMD5);
                client.info(key, {}, function (err2, info) {
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
    var top = '/' + client.user;

    client.ls(top, function (err, res) {
        t.ifError(err);
        t.ok(res);

        var objs = [];
        var dirs = [];

        res.on('object', function (obj) {
            objs.push(obj);
        });

        res.on('directory', function (dir) {
            dirs.push(dir);
        });

        res.once('error', function (err2) {
            t.ifError(err2);
            t.end();
        });

        res.once('end', function (httpRes) {
            t.ok(httpRes);
            assertMantaRes(t, httpRes, 200);

            t.equal(0, objs.length,
                'zero *objects* at top level `ls /:login`: objs=' + objs);

            var topdirs = new Set(dirs.map(function (d) { return d.name; }));

            t.ok(topdirs.has('stor'), 'have "stor" in topdirs');
            topdirs.delete('stor');

            t.ok(topdirs.has('public'), 'have "public" in topdirs');
            topdirs.delete('public');

            // *Might* have "reports" in topdirs. It is slated for removal
            // in mantav2.
            if (topdirs.has('reports')) {
                t.ok(topdirs.has('reports'), 'have "reports" in topdirs');
                topdirs.delete('reports');
            }

            // *Might* have "uploads" in topdirs. MPU support is disablable.
            if (topdirs.has('uploads')) {
                t.ok(topdirs.has('uploads'), 'have "uploads" in topdirs');
                topdirs.delete('uploads');
            }

            t.equal(topdirs.size, 0, 'expect no other top dirs, remaining=' +
                Array.from(topdirs).join(','));

            t.end();
        });
    });
});


test('ls top with marker', function (t) {
    var top = '/' + client.user;

    client.ls(top, { marker: 'public'}, function (err, res) {
        t.ifError(err);
        t.ok(res);

        var objs = [];
        var dirs = [];

        res.on('object', function (obj) {
            objs.push(obj);
        });

        res.on('directory', function (dir) {
            dirs.push(dir);
        });

        res.once('error', function (err2) {
            t.ifError(err2);
            t.end();
        });

        res.once('end', function (httpRes) {
            t.ok(httpRes);
            assertMantaRes(t, httpRes, 200);

            t.equal(0, objs.length);

            // We should expect to only see top-level dirs alphabetically after
            // our "public" marker name.
            var topdirs = new Set(dirs.map(function (d) { return d.name; }));

            t.ok(topdirs.has('stor'), 'have "stor" in topdirs');
            topdirs.delete('stor');

            // *Might* have "reports" in topdirs. It is slated for removal
            // in mantav2.
            if (topdirs.has('reports')) {
                t.ok(topdirs.has('reports'), 'have "reports" in topdirs');
                topdirs.delete('reports');
            }

            // *Might* have "uploads" in topdirs. MPU support is disablable.
            if (topdirs.has('uploads')) {
                t.ok(topdirs.has('uploads'), 'have "uploads" in topdirs');
                topdirs.delete('uploads');
            }

            t.equal(topdirs.size, 0, 'expect no other top dirs, remaining=' +
                Array.from(topdirs).join(','));

            t.end();
        });
    });
});


test('rmdir top', function (t) {
    var top = '/' + client.user;

    client.unlink(top, function (err, res) {
        t.ok(err);
        assertMantaRes(t, res, 400);
        t.equal(err.name, 'OperationNotAllowedOnRootDirectoryError');
        t.end();
    });
});


test('mkdir stor', function (t) {
    var stor = '/' + client.user + '/stor';

    client.mkdir(stor, function (err, res) {
        t.ifError(err, 'mkdir on /:login/stor should work');
        assertMantaRes(t, res, 204);
        t.end();
    });
});


test('mkdir no parent',  function (t) {
    var key = testDir + '/no-such-parent-dir/a-new-subdir';
    client.mkdir(key, function (err, res) {
        t.ok(err);
        assertMantaRes(t, res, 404);
        t.equal(err.name, 'DirectoryDoesNotExistError');
        t.end();
    });
});


test('put under non-directory', function (t) {
    var key = testDir + '/put-obj-under-non-directory';
    var key2 = key + '/obj-under-an-obj';

    writeObject(client, key, function (putErr) {
        t.ifError(putErr);

        client.mkdir(key2, function (err, res) {
            t.ok(err);
            assertMantaRes(t, res, 400);
            t.equal(err.name, 'ParentNotDirectoryError');
            t.end();
        });
    });
});


test('ls empty', function (t) {
    var emptyDir = testDir + '/empty-dir';

    client.mkdir(emptyDir, function (mkdirErr) {
        t.ifError(mkdirErr);

        client.ls(emptyDir, function (err, res) {
            t.ifError(err);
            t.ok(res);

            var objs = [];
            var dirs = [];

            res.on('object', function (obj) {
                objs.push(obj);
            });
            res.on('directory', function (dir) {
                dirs.push(dir);
            });
            res.once('error', function (err2) {
                t.ifError(err2, 'expected no "error" event');
                t.end();
            });
            res.once('end', function (httpRes) {
                t.ok(httpRes);
                assertMantaRes(t, httpRes, 200);
                t.equal(objs.length, 0);
                t.equal(dirs.length, 0);
                t.end();
            });
        });
    });
});

test('ls with obj and dir', function (t) {
    var subdirPath = testDir + '/obj-and-dir';
    var objPath = subdirPath + '/the-obj';
    var dirPath = subdirPath + '/the-dir';

    vasync.pipeline({
        funcs: [
            function theSubdir(_, next) {
                client.mkdir(subdirPath, next);
            },
            function theObj(_, next) {
                writeObject(client, objPath, next);
            },
            function theDir(_, next) {
                client.mkdir(dirPath, next);
            }
        ]
    }, function (err) {
        t.ifError(err);
        client.ls(subdirPath, function (err2, res) {
            t.ifError(err2);
            t.ok(res);

            var objs = [];
            var dirs = [];

            res.on('object', function (o) {
                t.ok(o);
                t.equal(o.type, 'object');
                t.ok(o.etag);
                t.ok(o.mtime);
                t.equal(o.name, objPath.split('/').pop());
                objs.push(o);
            });
            res.on('directory', function (d) {
                t.ok(d);
                t.equal(d.type, 'directory');
                t.ok(d.mtime);
                t.equal(d.name, dirPath.split('/').pop());
                dirs.push(d);
            });
            res.once('error', function (err3) {
                t.ifError(err3, 'expected no "error" ls event');
                t.end();
            });
            res.once('end', function (httpRes) {
                t.ok(httpRes);
                assertMantaRes(t, httpRes, 200);
                t.equal(objs.length, 1);
                t.equal(dirs.length, 1);
                t.end();
            });
        });
    });
});


test('ls escapes path', function (t) {
    var key = testDir + '/*needs=(escaping)';
    client.mkdir(key, function (err, res) {
        t.ifError(err);
        assertMantaRes(t, res, 204);

        client.ls(key, function (err2, res2) {
            t.ifError(err2);
            t.ok(res2);

            res2.once('error', function (err3) {
                t.ifError(err3);
                t.end();
            });
            res2.once('end', function (httpRes) {
                assertMantaRes(t, httpRes, 200);
                t.end();
            });
        });
    });
});


test('ls escapes marker', function (t) {
    var subdir = testDir + '/ls-escapes-marker';
    var firstName = 'aaa';
    var escapeyName = 'needs)*(=escaping';
    var lastName = 'zzz';

    vasync.pipeline({funcs: [
        function theDir(_, next) {
            client.mkdir(subdir, next);
        },
        function theFirstObj(_, next) {
            writeObject(client, subdir + '/' + firstName, next);
        },
        function theEscapeyObj(_, next) {
            writeObject(client, subdir + '/' + escapeyName, next);
        },
        function theLastObj(_, next) {
            writeObject(client, subdir + '/' + lastName, next);
        }
    ]}, function (setupErr) {
        t.ifError(setupErr);

        client.ls(subdir, { marker: escapeyName }, function (err, res) {
            t.ifError(err);
            t.ok(res);

            var objs = [];

            res.on('object', function (obj) {
                objs.push(obj);
            });
            res.on('directory', function (dir) {
                t.ok(false, 'should not be a directory under ' + subdir);
            });
            res.once('error', function (err2) {
                t.ifError(err2);
                t.end();
            });
            res.once('end', function (httpRes) {
                assertMantaRes(t, httpRes, 200);
                t.equal(2, objs.length);
                t.deepEqual(
                    objs.map(function (o) { return o.name; }).sort(),
                    [escapeyName, 'zzz']);
                t.end();
            });
        });
    });
});


test('ls 404', function (t) {
    client.ls(testDir + '/' + uuidv4(), function (err) {
        t.ok(err);
        t.equal(err.name, 'NotFoundError');
        t.end();
    });
});

test('rmdir', function (t) {
    var subdir = testDir + '/rmdir';

    client.mkdir(subdir, function (err) {
        t.ifError(err);

        client.unlink(subdir, function (err2, res) {
            t.ifError(err2);
            assertMantaRes(t, res, 204);
            t.end();
        });
    });
});


test('rmdir 404', function (t) {
    var subdir = testDir + '/rmdir-404-no-such-dir';

    client.unlink(subdir, function (err, res) {
        t.ok(err);
        t.equal(err.name, 'ResourceNotFoundError');
        assertMantaRes(t, res, 404);
        t.end();
    });
});

test('rmdir /:login/stor', function (t) {
    var stor = '/' + client.user + '/stor';
    client.unlink(stor, function (err, res) {
        t.ok(err);
        t.equal(err.name, 'OperationNotAllowedOnRootDirectoryError');
        assertMantaRes(t, res, 400);
        t.end();
    });
});

test('rmdir non-empty dir', function (t) {
    var key = testDir + '/rmdir-non-empty-dir-file';

    writeObject(client, key, function (putErr) {
        t.ifError(putErr);
        client.unlink(testDir, function (err, res) {
            t.ok(err);
            t.equal(err.name, 'DirectoryNotEmptyError');
            assertMantaRes(t, res, 400);
            t.end();
        });
    });
});

test('mkdir with Content-Disposition ignored', function (t) {
    var key = testDir + '/mkdir-with-content-disposition-ignored';
    var cd = 'attachment; filename="my-file.txt"';
    var opts = {
        headers: {
            'content-disposition': cd
        }
    };

    client.mkdir(key, opts, function (err, res) {
        t.ifError(err);
        assertMantaRes(t, res, 204);

        client.info(key, function (err2, info) {
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
    var key = testDir + '/mkdir-with-bad-content-disposition-ignored';
    var cd = 'attachment;"';
    var opts = {
        headers: {
            'content-disposition': cd
        }
    };

    client.mkdir(key, opts, function (err, res) {
        t.ifError(err);
        assertMantaRes(t, res, 204);

        client.info(key, function (err2, info) {
            t.ifError(err2);
            t.ok(info);
            t.ok(!('content-disposition' in info.headers),
                 'content-dispostion should not be in headers');
            t.end();
        });
    });
});

test('mkdir, chattr: content-disposition ignored', function (t) {
    var key = testDir + '/mkdir-chattr-content-disposition-ignored';
    var cd = 'attachment"';
    var opts = {
        headers: {
            'content-disposition': cd
        }
    };

    client.mkdir(key, function (err) {
        t.ifError(err);

        client.chattr(key, opts, function (err2) {
            t.ifError(err2);

            client.info(key, function (err3, info) {
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
    var key = testDir + '/mkdir-chattr-bad-content-disposition-ignored';
    var cd = 'attachment;"';
    var opts = {
        headers: {
            'content-disposition': cd
        }
    };

    client.mkdir(key, function (err) {
        t.ifError(err);

        client.chattr(key, opts, function (err2) {
            t.ifError(err2);

            client.info(key, function (err3, info) {
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
    var subdir = testDir + '/ls-returns-content-disp-for-non-streaming-objs';
    var key = subdir + '/an-obj';
    var cd = 'attachment; filename="my-file.txt"';
    var opts = {
        headers: {
            'content-disposition': cd
        }
    };

    client.mkdir(subdir, function (mkdirErr) {
        t.ifError(mkdirErr);

        writeObject(client, key, opts, function (putErr) {
            t.ifError(putErr);

            client.ls(subdir, function (err, res) {
                t.ifError(err);
                t.ok(res);

                var objs = [];

                res.on('object', function (obj) {
                    objs.push(obj);
                });

                res.once('error', function (err2) {
                    t.ifError(err2, 'do not expect "error" ls event');
                    t.end();
                });

                res.once('end', function (httpRes) {
                    assertMantaRes(t, httpRes, 200);
                    t.equal(objs.length, 1);
                    t.ok(objs[0].contentDisposition);
                    t.equal(objs[0].contentDisposition, cd);
                    t.end();
                });
            });
        });
    });
});

test('ls returns content-disposition for streaming objects', function (t) {
    var subdir = testDir + '/ls-returns-content-disp-for-streaming-objs';
    var key = subdir + '/an-obj';
    var cd = 'attachment; filename="my-file.txt"';
    var opts = {
        headers: {
            'content-disposition': cd
        }
    };

    client.mkdir(subdir, function (mkdirErr) {
        t.ifError(mkdirErr);

        writeStreamingObject(client, key, opts, function (putErr) {
            t.ifError(putErr);

            client.ls(subdir, function (err, res) {
                t.ifError(err);
                t.ok(res);

                var objs = [];

                res.on('object', function (obj) {
                    objs.push(obj);
                });

                res.once('error', function (err2) {
                    t.ifError(err2);
                    t.end();
                });

                res.once('end', function (httpRes) {
                    assertMantaRes(t, httpRes, 200);
                    t.equal(objs.length, 1);
                    t.ok(objs[0].contentDisposition);
                    t.equal(objs[0].contentDisposition, cd);
                    t.end();
                });
            });
        });
    });
});

test('ls returns no content-disposition when not set', function (t) {
    var subdir = testDir + '/ls-returns-no-content-disp-when-not-set';
    var key = subdir + '/an-obj';

    client.mkdir(subdir, function (mkdirErr) {
        t.ifError(mkdirErr);

        writeObject(client, key, function (putErr) {
            t.ifError(putErr);

            client.ls(subdir, function (err, res) {
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

                res.once('end', function (httpRes) {
                    assertMantaRes(t, httpRes, 200);
                    t.equal(objs.length, 1, 'one object in list');
                    t.ok(!(objs[0].contentDisposition),
                         'no content disposition field');
                    t.end();
                });
            });
        });
    });
});


test('teardown', function (t) {
    client.rmr(testDir, function onRm(err) {
        t.ifError(err, 'remove testDir: ' + testDir);
        t.end();
    });
});
