/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var crypto = require('crypto');

var assert = require('assert-plus');
var MemoryStream = require('stream').PassThrough;
var test = require('tap').test;
var uuidv4 = require('uuid/v4');
var vasync = require('vasync');

var helper = require('../helper');



///--- Globals

var assertMantaRes = helper.assertMantaRes;
var client;
var testAccount;
var testDir;
var TEXT = 'The lazy brown fox \nsomething \nsomething foo';


///--- Helpers

function writeObject(client, key, opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    var putOpts = {
        headers: opts.headers,
        md5: crypto.createHash('md5').update(TEXT).digest('base64'),
        size: Buffer.byteLength(TEXT),
        type: 'text/plain'
    };
    var stream = new MemoryStream();

    client.put(key, stream, putOpts, cb);
    setImmediate(stream.end.bind(stream, TEXT));
}


function assertObjContent(opts, cb) {
    assert.object(opts.t, 'opts.t');
    assert.object(opts.stream, 'opts.stream');
    assert.object(opts.res, 'opts.res');
    assert.number(opts.code, 'opts.code');
    assert.string(opts.text, 'opts.text');
    assert.optionalString(opts.contentType, 'opts.contentType');
    assert.func(cb, 'cb');

    var res = opts.res;
    var stream = opts.stream;
    var t = opts.t;

    assertMantaRes(t, res, opts.code);
    t.ok(res.headers.etag, 'response has "etag" header');
    t.ok(res.headers['last-modified'], 'response has "last-modified" header');
    if (opts.contentType) {
        t.equal(res.headers['content-type'], opts.contentType,
            'response "content-type" is ' + opts.contentType);
    }

    stream.setEncoding('utf8');
    var body = '';
    stream.on('data', function (chunk) {
        body += chunk;
    });
    stream.once('error', function (err) {
        t.ifError(err);
        cb();
    });
    stream.once('end', function () {
        t.equal(body, opts.text);
        cb();
    });

    stream.resume();
}

function putObjectAndCheckRes(t, client, key, opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    writeObject(client, key, opts, function (err, res) {
        t.ifError(err);
        assertMantaRes(t, res, 204);
        cb(null, res.headers);
    });
}

///--- Tests

test('setup: test account', function (t) {
    helper.ensureTestAccounts(t, function (err, accounts) {
        t.ifError(err, 'no error loading/creating test accounts');
        testAccount = accounts.regular;
        t.ok(testAccount, 'have regular test account: ' + testAccount.login);
        t.end();
    });
});

test('setup: test dir', function (t) {
    client = helper.mantaClientFromAccountInfo(testAccount);
    testDir = '/' + testAccount.login + '/stor/test-obj-' +
        uuidv4().split('-')[0]

    client.mkdir(testDir, function (err) {
        t.ifError(err, 'no error making testDir:' + testDir);
        t.end();
    });
});


test('put object', function (t) {
    var key = testDir + '/put-object';
    var stream = new MemoryStream();
    var size = Buffer.byteLength(TEXT);
    setImmediate(stream.end.bind(stream, TEXT));

    client.put(key, stream, {size: size}, function (err, res) {
        t.ifError(err);
        assertMantaRes(t, res, 204);
        t.end();
    });
});


test('put overwrite', function (t) {
    var key = testDir + '/put-overwrite';
    putObjectAndCheckRes(t, client, key, function (_, headers) {
        putObjectAndCheckRes(t, client, key, function (__, headers2) {
            t.notEqual(headers.etag, headers2.etag, 'etags differ');
            t.end();
        });
    });
});


test('put/get zero bytes', function (t) {
    var key = testDir + '/put-get-zero-bytes';
    var stream = new MemoryStream();
    setImmediate(stream.end.bind(stream));

    client.put(key, stream, {size: 0}, function (err, res) {
        t.ifError(err);
        if (err) {
            t.end();
            return;
        }
        assertMantaRes(t, res, 204);
        client.get(key, function (err2, stream2, res2) {
            t.ifError(err2);
            assertMantaRes(t, res2, 200);
            t.equal(res2.headers['content-md5'], '1B2M2Y8AsgTpgAmY7PhCfg==');
            t.ok(stream2);
            if (stream2) {
                stream2.once('end', t.end.bind(t));
                stream2.resume();
            } else {
                t.end();
                return;
            }
        });
    });
});

test('put streaming object', function (t) {
    var key = testDir + '/put-streaming-object';
    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';
    var size = Buffer.byteLength(text);

    setImmediate(stream.end.bind(stream, text));
    client.put(key, stream, function (err, res) {
        t.ifError(err);
        assertMantaRes(t, res, 204);
        client.get(key, function (err2, stream2, res2) {
            t.ifError(err2);
            assertMantaRes(t, res2, 200);
            t.equal(Number(res2.headers['content-length']), size);
            var body = '';
            stream2.setEncoding('utf8');
            stream2.on('data', function (buf) {
                body += buf;
            });
            stream2.once('end', function () {
                t.equal(text, body);
                t.end();
            });
            stream2.resume();
        });
    });
});


test('put streaming object exceed max promised size', function (t) {
    var key = testDir + '/put-streaming-object-exceed-size';
    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';

    setImmediate(stream.end.bind(stream, text));
    var opts = {
        headers: {
            'max-content-length': 1
        }
    };
    client.put(key, stream, opts, function (err, res) {
        t.ok(err);
        t.equal(err.name, 'MaxContentLengthExceededError');
        assertMantaRes(t, res, 413);
        t.end();
    });
});


test('put object (1 copy)', function (t) {
    var key = testDir + '/put-obj-1-copy';
    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';
    var size = Buffer.byteLength(text);
    var _opts = {
        copies: 1,
        size: size
    };
    setImmediate(stream.end.bind(stream, text));

    client.put(key, stream, _opts, function (err, res) {
        t.ifError(err);
        assertMantaRes(t, res, 204);
        t.end();
    });
});



test('put object (3 copies)', function (t) {
    var key = testDir + '/put-obj-3-copies';
    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';
    var size = Buffer.byteLength(text);
    var _opts = {
        copies: 3,
        size: size
    };
    setImmediate(stream.end.bind(stream, text));

    // This fails against COAL
    client.put(key, stream, _opts, function (err, res) {
        if (err) {
            t.equal(err.name, 'NotEnoughSpaceError');
            assertMantaRes(t, res, 507);
        } else {
            assertMantaRes(t, res, 204);
        }
        t.end();
    });
});


// Default maximum is 9 copies.
test('put object (10 copies)', function (t) {
    var key = testDir + '/put-obj-10-copies';
    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';
    var size = Buffer.byteLength(text);
    var _opts = {
        copies: 10,
        size: size
    };
    setImmediate(stream.end.bind(stream, text));

    client.put(key, stream, _opts, function (err, res) {
        t.ok(err);
        if (err)
            t.equal(err.name, 'InvalidDurabilityLevelError');
        assertMantaRes(t, res, 400);
        t.end();
    });
});


test('chattr: m- headers', function (t) {
    var key = testDir + '/chattr-m-headers';
    var opts = {
        headers: {
            'm-foo': 'bar',
            'm-bar': 'baz'
        }
    };

    putObjectAndCheckRes(t, client, key, function () {
        client.chattr(key, opts, function (err) {
            t.ifError(err);

            client.info(key, function (err2, info) {
                t.ifError(err2);
                t.ok(info);
                if (info) {
                    var h = info.headers || {};
                    t.equal(h['m-foo'], 'bar');
                    t.equal(h['m-bar'], 'baz');
                }
                t.end();
            });
        });
    });
});


test('chattr: content-type', function (t) {
    var key = testDir + '/chattr-content-type';
    var opts = {
        headers: {
            'content-type': 'jpg'
        }
    };

    putObjectAndCheckRes(t, client, key, function () {
        client.chattr(key, opts, function (err) {
            t.ifError(err);
            t.ifError(err);

            client.info(key, function (err2, info) {
                t.ifError(err2);
                t.ok(info);
                if (info) {
                    var h = info.headers || {};
                    t.equal(h['content-type'], 'image/jpeg');
                }
                t.end();
            });
        });
    });
});



test('chattr: bogus durability-level', function (t) {
    var key = testDir + '/chattr-bogus-durability-level';
    var opts = {
        headers: {
            'durability-level': '4'
        }
    };

    putObjectAndCheckRes(t, client, key, function () {
        client.chattr(key, opts, function (err) {
            t.ok(err);
            t.equal(err.name, 'InvalidUpdateError');
            t.end();
        });
    });
});


test('chattr: bogus content-md5', function (t) {
    var key = testDir + '/chattr-bogus-content-md5';
    var opts = {
        headers: {
            'content-md5': 'foo'
        }
    };

    putObjectAndCheckRes(t, client, key, function () {
        client.chattr(key, opts, function (err) {
            t.ok(err);
            t.equal(err.name, 'InvalidUpdateError');
            t.end();
        });
    });
});


test('chattr: bogus content-length', function (t) {
    var key = testDir + '/chattr-bogus-content-length';
    var opts = {
        headers: {
            'content-length': '4'
        }
    };

    putObjectAndCheckRes(t, client, key, function () {
        client.chattr(key, opts, function (err) {
            t.ok(err);
            t.equal(err.name, 'InvalidUpdateError');
            t.end();
        });
    });
});


test('chattr: no object', function (t) {
    var key = testDir + '/chattr-no-object';
    var opts = {
        headers: {
            'm-foo': 'bar'
        }
    };

    client.chattr(key, opts, function (err) {
        t.ok(err);
        t.equal(err.name, 'ResourceNotFoundError');
        t.end();
    });
});



test('MANTA-625 (custom headers)', function (t) {
    var key = testDir + '/MANTA-625-custom-headers';
    var opts = {
        headers: {
            'm-foo': 'bar'
        }
    };

    putObjectAndCheckRes(t, client, key, opts, function () {
        client.get(key, function (err, stream, res) {
            t.ifError(err);
            assertMantaRes(t, res, 200);
            t.equal(res.headers['m-foo'], 'bar');
            stream.once('end', t.end.bind(t));
            stream.resume();
        });
    });
});


test('put if-match ok', function (t) {
    var key = testDir + '/put-if-match-ok';
    putObjectAndCheckRes(t, client, key, function (_, headers) {
        var etag = headers.etag;
        var opts = {
            headers: {
                'if-match': etag
            }
        };
        putObjectAndCheckRes(t, client, key, opts, function (__, headers2) {
            t.notEqual(etag, headers2.etag, 'etags differ');
            t.end();
        });
    });
});


test('put if-match fail', function (t) {
    var key = testDir + '/put-if-match-fail';
    putObjectAndCheckRes(t, client, key, function () {
        var opts = {
            headers: {
                'if-match': uuidv4()
            }
        };
        writeObject(client, key, opts, function (err, res) {
            t.ok(err);
            t.equal(err.name, 'PreconditionFailedError');
            assertMantaRes(t, res, 412);
            t.end();
        });
    });
});


test('put if-none-match ok', function (t) {
    var key = testDir + '/put-if-none-match-ok';
    putObjectAndCheckRes(t, client, key, function (_, headers) {
        var opts = {
            headers: {
                'if-none-match': uuidv4()
            }
        };
        putObjectAndCheckRes(t, client, key, opts, function (__, headers2) {
            t.notEqual(headers.etag, headers2.etag, 'etags differ');
            t.end();
        });
    });
});


test('put if-none-match fail', function (t) {
    var key = testDir + '/put-if-none-match-fail';
    var etag;
    putObjectAndCheckRes(t, client, key, function (_, headers) {
        etag = headers.etag;
        var opts = {
            headers: {
                'if-none-match': etag
            }
        };
        writeObject(client, key, opts, function (err, res) {
            t.ok(err);
            t.equal(err.name, 'PreconditionFailedError');
            assertMantaRes(t, res, 412);
            t.end();
        });
    });
});


test('put unmodified-since ok', function (t) {
    var key = testDir + '/put-unmodified-since-ok';
    putObjectAndCheckRes(t, client, key, function (_, headers) {
        var date = headers['last-modified'];
        var opts = {
            headers: {
                'if-unmodified-since': date
            }
        };
        putObjectAndCheckRes(t, client, key, opts, function (__, headers2) {
            t.notEqual(headers.etag, headers2.etag, 'etags differ');
            t.end();
        });
    });
});


test('put unmodified-since fail', function (t) {
    var key = testDir + '/put-unmodified-since-fail';
    putObjectAndCheckRes(t, client, key, function (_, headers) {
        var d = new Date(Date.parse(headers['last-modified']) - 10000);
        var old = new Date(d).toUTCString();
        var opts = {
            headers: {
                'if-unmodified-since': old
            }
        };
        writeObject(client, key, opts, function (err, res) {
            t.ok(err);
            t.equal(err.name, 'PreconditionFailedError');
            assertMantaRes(t, res, 412);
            t.end();
        });
    });
});


test('put bad content-md5', function (t) {
    var key = testDir + '/put-bad-content-md5';
    var opts = {
        md5: 'bogus',
        size: Buffer.byteLength(TEXT),
        type: 'text/plain'
    };
    var stream = new MemoryStream();

    client.put(key, stream, opts, function (err, res) {
        t.ok(err);
        t.equal(err.name, 'BadRequestError');
        assertMantaRes(t, res, 400);
        t.end();
    });
    setImmediate(stream.end.bind(stream, TEXT));
});


test('put parent ENOEXIST', function (t) {
    var key = testDir + '/no-such-parent-dir/the-obj';
    writeObject(client, key, function (err, res) {
        t.ok(err);
        t.equal(err.name, 'DirectoryDoesNotExistError');
        assertMantaRes(t, res, 404);
        t.end();
    });
});


test('put parent not directory', function (t) {
    var key = testDir + '/put-parent-not-directory';
    putObjectAndCheckRes(t, client, key, function () {
        var k = key + '/' + uuidv4();
        writeObject(client, k, function (err, res) {
            t.ok(err);
            t.equal(err.name, 'ParentNotDirectoryError');
            assertMantaRes(t, res, 400);
            t.end();
        });
    });
});


test('put too big', function (t) {
    var key = testDir + '/put-too-big';
    var opts = {
        size: 1000000000000000,
        type: 'text/plain'
    };
    var stream = new MemoryStream();

    client.put(key, stream, opts, function (err, res) {
        t.ok(err);
        t.equal(err.name, 'NotEnoughSpaceError');
        assertMantaRes(t, res, 507);
        t.end();
    });
});


test('get ok', function (t) {
    var key = testDir + '/get-ok';
    putObjectAndCheckRes(t, client, key, function () {
        client.get(key, function (err, stream, res) {
            t.ifError(err);
            t.equal('bytes', res.headers['accept-ranges']);
            assertObjContent({
                t: t,
                stream: stream,
                res: res,
                code: 200,
                text: TEXT,
                contentType: 'text/plain'
            }, function () {
                t.end();
            });
        });
    });
});


test('get 404', function (t) {
    var key = testDir + '/get-404';
    client.get(key + 'a', function (err, stream, res) {
        t.ok(err);
        t.equal(err.name, 'ResourceNotFoundError');
        assertMantaRes(t, res, 404);
        t.end();
    });
});


test('get 406', function (t) {
    var key = testDir + '/get-406';
    putObjectAndCheckRes(t, client, key, function () {
        var opts = {
            accept: 'application/json'
        };
        client.get(key, opts, function (err, stream, res) {
            t.ok(err);
            t.equal(err.name, 'NotAcceptableError');
            assertMantaRes(t, res, 406);
            t.end();
        });
    });
});


test('get if-match ok', function (t) {
    var key = testDir + '/get-if-match-ok';
    putObjectAndCheckRes(t, client, key, function (_, headers) {
        var opts = {
            headers: {
                'if-match': headers.etag
            }
        };
        client.get(key, opts, function (err, stream, res) {
            t.ifError(err);
            assertMantaRes(t, res, 200);
            t.end();
        });
    });
});


test('get if-match fail', function (t) {
    var key = testDir + '/get-if-match-fail';
    putObjectAndCheckRes(t, client, key, function () {
        var opts = {
            headers: {
                'if-match': uuidv4()
            }
        };
        client.get(key, opts, function (err, stream, res) {
            t.ok(err);
            t.equal(err.name, 'PreconditionFailedError');
            t.equal(stream, null);
            assertMantaRes(t, res, 412);
            t.end();
        });
    });
});


test('get 304', function (t) {
    var key = testDir + '/get-304';
    putObjectAndCheckRes(t, client, key, function (_, headers) {
        var opts = {
            headers: {
                'if-none-match': headers.etag
            }
        };
        client.get(key, opts, function (err, stream, res) {
            t.ifError(err);
            assertMantaRes(t, res, 304);
            t.equal(stream, null);
            t.end();
        });
    });
});


test('get if-none-match ok', function (t) {
    var key = testDir + '/get-if-none-match-ok';
    putObjectAndCheckRes(t, client, key, function () {
        var opts = {
            headers: {
                'if-none-match': uuidv4()
            }
        };
        client.get(key, opts, function (err, stream, res) {
            t.ifError(err);
            assertMantaRes(t, res, 200);
            t.end();
        });
    });
});


test('get if-modified-since ok (data)', function (t) {
    var key = testDir + '/get-if-modified-since-ok-data';
    putObjectAndCheckRes(t, client, key, function () {
        var d = new Date(1).toUTCString();
        var opts = {
            headers: {
                'if-modified-since': d
            }
        };
        client.get(key, opts, function (err, stream, res) {
            t.ifError(err);
            assertMantaRes(t, res, 200);
            t.end();
        });
    });
});


test('get if-modified-since 304', function (t) {
    var key = testDir + '/get-if-modified-since-304';
    putObjectAndCheckRes(t, client, key, function () {
        var d = new Date(Date.now() + 10000).toUTCString();
        var opts = {
            headers: {
                'if-modified-since': d
            }
        };
        client.get(key, opts, function (err, stream, res) {
            t.ifError(err);
            assertMantaRes(t, res, 304);
            t.end();
        });
    });
});


test('get if-unmodified-since 200', function (t) {
    var key = testDir + '/get-if-unmodified-since-200';
    putObjectAndCheckRes(t, client, key, function (_, headers) {
        var opts = {
            headers: {
                'if-unmodified-since': headers['last-modified']
            }
        };
        client.get(key, opts, function (err, stream, res) {
            t.ifError(err);
            assertMantaRes(t, res, 200);
            t.end();
        });
    });
});


test('get if-unmodified-since 412', function (t) {
    var key = testDir + '/get-if-unmodified-since-412';
    putObjectAndCheckRes(t, client, key, function (_, headers) {
        var d = new Date(Date.now() - 100000).toUTCString();
        var opts = {
            headers: {
                'if-unmodified-since': d
            }
        };
        client.get(key, opts, function (err, stream, res) {
            t.ok(err);
            t.equal(err.name, 'PreconditionFailedError');
            assertMantaRes(t, res, 412);
            t.end();
        });
    });
});


test('get range', function (t) {
    var key = testDir + '/get-range';
    var stream = new MemoryStream();
    var text = 'abcdefghijklmnopqrstuvwxyz';
    var size = Buffer.byteLength(text);
    setImmediate(stream.end.bind(stream, text));

    client.put(key, stream, {size: size}, function (err, res) {
        var opts = {
            headers: {
                'range': 'bytes=3-8'
            }
        };
        client.get(key, opts, function (err2, s, r) {
            t.equal(undefined, r.headers['accept-ranges']);
            t.equal(undefined, r.headers['content-md5']);
            t.equal(6, Number(r.headers['content-length']));
            t.ok(r.headers.etag);
            t.equal('application/octet-stream',
                    r.headers['content-type']);
            t.equal('bytes 3-8/26', r.headers['content-range']);
            assertObjContent({
                t: t,
                stream: s,
                res: r,
                code: 206,
                text: 'defghi'
            }, function () {
                t.end();
            });
        });
    });
});


test('get range, prefix', function (t) {
    var key = testDir + '/get-range-prefix';
    var stream = new MemoryStream();
    var text = 'abcdefghijklmnopqrstuvwxyz';
    var size = Buffer.byteLength(text);
    setImmediate(stream.end.bind(stream, text));

    client.put(key, stream, {size: size}, function (err, res) {
        var opts = {
            headers: {
                'range': 'bytes=19-'
            }
        };
        client.get(key, opts, function (err2, s, r) {
            t.equal(Number(r.headers['content-length']), 7);
            t.equal('bytes 19-25/26', r.headers['content-range']);
            assertObjContent({
                t: t,
                stream: s,
                res: r,
                code: 206,
                text: 'tuvwxyz'
            }, function () {
                t.end();
            });
        });
    });
});


test('get range, suffix', function (t) {
    var key = testDir + '/get-range-suffix';
    var stream = new MemoryStream();
    var text = 'abcdefghijklmnopqrstuvwxyz';
    var size = Buffer.byteLength(text);
    setImmediate(stream.end.bind(stream, text));

    client.put(key, stream, {size: size}, function (err, res) {
        var opts = {
            headers: {
                'range': 'bytes=-10'
            }
        };
        client.get(key, opts, function (err2, s, r) {
            t.equal(Number(r.headers['content-length']), 10);
            t.equal('bytes 16-25/26', r.headers['content-range']);
            assertObjContent({
                t: t,
                stream: s,
                res: r,
                code: 206,
                text: 'qrstuvwxyz'
            }, function () {
                t.end();
            });
        });
    });
});


test('get range, multi-range', function (t) {
    var key = testDir + '/get-range-multi-range';
    var stream = new MemoryStream();
    var text = 'abcdefghijklmnopqrstuvwxyz';
    var size = Buffer.byteLength(text);
    setImmediate(stream.end.bind(stream, text));

    client.put(key, stream, {size: size}, function (err, res) {
        var opts = {
            headers: {
                'range': 'bytes=0-5,6-10'
            }
        };
        client.get(key, opts, function (err2, s, r) {
            t.equal(501, r.statusCode);
            t.end();
        });
    });
});


test('get invalid range', function (t) {
    var key = testDir + '/get-invalid-range';
    var stream = new MemoryStream();
    var text = 'abcdefghijklmnopqrstuvwxyz';
    var size = Buffer.byteLength(text);
    setImmediate(stream.end.bind(stream, text));

    client.put(key, stream, {size: size}, function (err, res) {
        var opts = {
            headers: {
                'range': 'bytes=foo'
            }
        };
        client.get(key, opts, function (err2, s, r) {
            t.equal(416, r.statusCode);
            t.equal('bytes */26', r.headers['content-range']);
            t.end();
        });
    });
});


test('get range, out of bounds', function (t) {
    var key = testDir + '/get-range-out-of-bounds';
    var stream = new MemoryStream();
    var text = 'abcdefghijklmnopqrstuvwxyz';
    var size = Buffer.byteLength(text);
    setImmediate(stream.end.bind(stream, text));

    client.put(key, stream, {size: size}, function (err, res) {
        var opts = {
            headers: {
                'range': 'bytes=27-100'
            }
        };
        client.get(key, opts, function (err2, s, r) {
            t.equal(416, r.statusCode);
            t.equal('bytes */26', r.headers['content-range']);
            t.end();
        });
    });
});


test('del ok', function (t) {
    var key = testDir + '/del-ok';
    putObjectAndCheckRes(t, client, key, function () {
        client.unlink(key, function (err, res) {
            t.ifError(err);
            assertMantaRes(t, res, 204);
            t.end();
        });
    });
});


test('del 404', function (t) {
    var key = testDir + '/del-404';
    client.unlink(key + 'a', function (err, res) {
        t.ok(err);
        t.equal(err.name, 'ResourceNotFoundError');
        assertMantaRes(t, res, 404);
        t.end();
    });
});


test('del if-match ok', function (t) {
    var key = testDir + '/del-if-match-ok';
    putObjectAndCheckRes(t, client, key, function (_, headers) {
        var etag = headers.etag;
        var opts = {
            headers: {
                'if-match': etag
            }
        };
        client.unlink(key, opts, function (err, res) {
            t.ifError(err);
            assertMantaRes(t, res, 204);
            t.end();
        });
    });
});


test('del if-match fail', function (t) {
    var key = testDir + '/del-if-match-fail';
    putObjectAndCheckRes(t, client, key, function () {
        var opts = {
            headers: {
                'if-match': uuidv4()
            }
        };
        client.unlink(key, opts, function (err, res) {
            t.ok(err);
            t.equal(err.name, 'PreconditionFailedError');
            assertMantaRes(t, res, 412);
            t.end();
        });
    });
});


test('del if-none-match ok', function (t) {
    var key = testDir + '/del-if-none-match-ok';
    putObjectAndCheckRes(t, client, key, function () {
        var opts = {
            headers: {
                'if-none-match': uuidv4()
            }
        };
        client.unlink(key, opts, function (err, res) {
            t.ifError(err);
            assertMantaRes(t, res, 204);
            t.end();
        });
    });
});


test('del if-none-match fail', function (t) {
    var key = testDir + '/del-if-none-match-fail';
    putObjectAndCheckRes(t, client, key, function (_, headers) {
        var opts = {
            headers: {
                'if-none-match': headers.etag
            }
        };
        client.unlink(key, opts, function (err, res) {
            t.ok(err);
            t.equal(err.name, 'PreconditionFailedError');
            assertMantaRes(t, res, 412);
            t.end();
        });
    });
});


// content-disposition tests

test('Put-Get no content-disposition', function (t) {
    var key = testDir + '/put-get-no-content-disposition';
    var opts = {};
    putObjectAndCheckRes(t, client, key, opts, function (_, headers) {
        client.get(key, opts, function (err, stream, res) {
            t.ifError(err);
            assertMantaRes(t, res, 200);
            t.ok(!('content-disposition' in res.headers),
                 'No content disposition expected');
            t.end();
        });
    });
});

test('Put-Get content-disposition', function (t) {
    var key = testDir + '/put-get-content-disposition';
    var cd = 'attachment; filename="my-file.txt"';
    var opts = {
        headers: {
            'content-disposition': cd
        }
    };

    putObjectAndCheckRes(t, client, key, opts, function (_, headers) {
        client.get(key, opts, function (err, stream, res) {
            t.ifError(err);
            assertMantaRes(t, res, 200);
            t.equal(res.headers['content-disposition'], cd,
                    'Content-Disposition should match written value');
            t.end();
        });
    });
});

test('Put-Get content-disposition cleaned', function (t) {
    var key = testDir + '/put-get-content-disposition-cleaned';
    var cd = 'attachment; filename="/root/my-file.txt"';
    var opts = {
        headers: {
            'content-disposition': cd
        }
    };

    putObjectAndCheckRes(t, client, key, opts, function (_, headers) {
        client.get(key, opts, function (err, stream, res) {
            t.ifError(err);
            assertMantaRes(t, res, 200);
            t.equal(res.headers['content-disposition'],
                    'attachment; filename="my-file.txt"',
                    'Content-Disposition should be clean');
            t.end();
        });
    });
});

test('streaming object valid content-disposition',
     function (t) {
    var key = testDir + '/streaming-object-valid-content-disposition';
         var stream = new MemoryStream();
         var text = 'The lazy brown fox \nsomething \nsomething foo';

         setImmediate(stream.end.bind(stream, text));
         var opts = {
             headers: {
                 'content-disposition': 'attachment; filename="my-file.txt"'
             }
         };

         client.put(key, stream, opts, function (err, res) {
             t.ifError(err);
             assertMantaRes(t, res, 204);
             t.end();
         });
     });

test('streaming object invalid content-disposition',
     function (t) {
    var key = testDir + '/streaming-object-invalid-content-disposition';
         var stream = new MemoryStream();
         var text = 'The lazy brown fox \nsomething \nsomething foo';

         setImmediate(stream.end.bind(stream, text));
         var opts = {
             headers: {
                 'content-disposition': 'attachment;'
             }
         };
         client.put(key, stream, opts, function (err, res) {
             t.equal(res.statusCode, 400, 'Expected 400');
             t.equal(err.name, 'BadRequestError', 'Expected a BadRequestError');
             t.end();
         });
     });

test('chattr: valid content-disposition', function (t) {
    var key = testDir + '/chattr-valid-content-disposition';
    var cd = 'attachment; filename="my-file.txt"';
    var opts = {
        headers: {
            'content-disposition': cd
        }
    };


    putObjectAndCheckRes(t, client, key, function () {
        client.chattr(key, opts, function (err) {
            t.ifError(err);

            client.info(key, function (err2, info) {
                t.ifError(err2);
                t.ok(info);
                if (info) {
                    var h = info.headers || {};
                    t.equal(h['content-disposition'], cd,
                            'Content-Disposition should match written value');
                }
                t.end();
            });
        });
    });
});

test('chattr invalid content-disposition',
     function (t) {
    var key = testDir + '/chattr-invalid-content-disposition';
         var opts = {
             headers: {
                 'content-disposition': 'attachment;'
             }
         };

         putObjectAndCheckRes(t, client, key, function () {
             client.chattr(key, opts, function (err, res) {
                 t.equal(res.statusCode, 400, 'Expected 400');
                 t.equal(err.name, 'BadRequestError',
                         'Expected a BadRequestError');
                 t.end();
             });
         });
     });

// content-type tests

/*
 * Verify that a write with unknown content-type with a valid format succeeds
 * and server returns valid content-type on read of the same object.
 */
test('MANTA-4133 (non-existent content-type)', function (t) {
    var key = testDir + '/MANTA-4133-non-existent-content-type';
    var opts = {
        headers: {
            'content-type': 'argle/'
        }
    };

    writeObject(client, key, opts, function (err, res) {
        t.ifError(err);
        assertMantaRes(t, res, 204);
        client.info(key, function (err2, stream, res2) {
            t.ifError(err2);
            assertMantaRes(t, res2, 200);
            t.equal(res2.headers['content-type'], 'application/octet-stream');
            t.end();
        });
    });
});

/*
 * Verify that a write with a valid content-type with a valid format succeeds
 * and server returns valid content-type on read of the same object.
 */
test('MANTA-4133 (verify valid json content-type)', function (t) {
    var key = testDir + '/MANTA-4133-verify-valid-json-content-type';

    var opts = {
        headers: {
            'content-type': 'application/json'
        }
    };

    writeObject(client, key, opts, function (err, res) {
        t.ifError(err);
        assertMantaRes(t, res, 204);
        client.get(key, function (err2, stream, res2) {
            t.ifError(err2);
            assertMantaRes(t, res2, 200);
            t.equal(res2.headers['content-type'], 'application/json');
            t.end();
        });
    });
});

/*
 * Verify that a write with an malformed content-type succeeds
 * and server returns valid content-type on read of the same object.
 */
test('MANTA-4133 (malformed content-type)', function (t) {
    var key = testDir + '/MANTA-4133-malformed-content-type';
    var opts = {
        headers: {
            'content-type': '/*'
        }
    };

    writeObject(client, key, opts, function (err, res) {
        t.ifError(err);
        assertMantaRes(t, res, 204);
        client.info(key, function (err2, stream, res2) {
            t.ifError(err2);
            assertMantaRes(t, res2, 200);
            t.equal(res2.headers['content-type'], 'application/octet-stream');
            t.end();
        });
    });
});

/*
 * Verify that a write with an empty content-type succeeds and server
 * returns valid content-type on read of the same object.
 * Note: the change in 4133 should detect the absent content-type. The lookup
 * should fail and return 'application/octet-stream', the default.
 * The server returns 'text/plain' with the response as before.
 */
test('MANTA-4133 (empty content-type)', function (t) {
    var key = testDir + '/MANTA-4133-empty-content-type';
    var opts = {
        headers: {
            'content-type': ''
        }
    };

    writeObject(client, key, opts, function (err, res) {
        t.ifError(err);
        assertMantaRes(t, res, 204);
        client.info(key, function (err2, stream, res2) {
            t.ifError(err2);
            assertMantaRes(t, res2, 200);
            t.equal(res2.headers['content-type'], 'text/plain');
            t.end();
        });
    });
});

/*
 * Verify that a write with a valid content-type with a valid format succeeds
 * and server returns valid content-type on read of the same object.
 */
test('MANTA-4133 (verify valid plain text content-type)', function (t) {
    var key = testDir + '/MANTA-4133-verify-valid-plain-text-content-type';

    var opts = {
        headers: {
            'content-type': 'text/plain'
        }
    };

    writeObject(client, key, opts, function (err, res) {
        t.ifError(err);
        assertMantaRes(t, res, 204);
        client.info(key, function (err2, stream, res2) {
            t.ifError(err2);
            assertMantaRes(t, res2, 200);
            t.equal(res2.headers['content-type'], 'text/plain');
            t.end();
        });
    });
});

/*
 * Verify that a write with unknown content-type with a valid format succeeds
 * and server returns valid content-type on read of the same object.
 */
test('MANTA-4133 (verify non-existent utf-8 content-type)', function (t) {
    var key = testDir + '/MANTA-4133-verify-non-existent-utf-8-content-type';
    var encoded = '%EC%95%88%EB%85%95%ED%95%98%EC%84%B8%EC%9A%94';
    var ct_utf8 = unescape(encoded);

    var opts = {
        headers: {
            'content-type': ct_utf8
        }
    };

    writeObject(client, key, opts, function (err, res) {
        t.ifError(err);
        assertMantaRes(t, res, 204);
        client.info(key, function (err2, stream, res2) {
            t.ifError(err2);
            assertMantaRes(t, res2, 200);
            t.equal(res2.headers['content-type'], 'application/octet-stream');
            t.end();
        });
    });
});

/*
 * Verify that a write properly formed content-type with a valid format succeeds
 * and server returns the content-type on read of the same object.
 */
test('MANTA-4133 (verify a conforming content-type)', function (t) {
    var key = testDir + '/MANTA-4133-verify-a-conforming-content-type';

    var opts = {
        headers: {
            'content-type': 'audio/mpeg'
        }
    };

    writeObject(client, key, opts, function (err, res) {
        t.ifError(err);
        assertMantaRes(t, res, 204);
        client.info(key, function (err2, stream, res2) {
            t.ifError(err2);
            assertMantaRes(t, res2, 200);
            t.equal(res2.headers['content-type'], 'audio/mpeg');
            t.end();
        });
    });
});

test('teardown', function (t) {
    client.rmr(testDir, function onRm(err) {
        t.ifError(err, 'remove testDir: ' + testDir);
        t.end();
    });
});
