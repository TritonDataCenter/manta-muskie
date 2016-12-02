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
var restify = require('restify');
var uuid = require('node-uuid');
var vasync = require('vasync');

if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');



///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;

var TEXT = 'The lazy brown fox \nsomething \nsomething foo';



///--- Helpers

function writeObject(client, key, opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    var _opts = {
        headers: opts.headers,
        md5: crypto.createHash('md5').update(TEXT).digest('base64'),
        size: Buffer.byteLength(TEXT),
        type: 'text/plain'
    };
    var stream = new MemoryStream();

    client.put(key, stream, _opts, cb);
    process.nextTick(stream.end.bind(stream, TEXT));
}



///--- Tests

before(function (cb) {
    var self = this;

    this.client = helper.createClient();
    this.root = '/' + this.client.user + '/stor';
    this.dir = this.root + '/' + uuid.v4();
    this.key = this.dir + '/' + uuid.v4();
    this.putObject = function putObject(t, opts, _cb) {
        if (typeof (opts) === 'function') {
            _cb = opts;
            opts = {};
        }
        writeObject(self.client, self.key, opts, function (err, res) {
            t.ifError(err);
            t.ok(res);
            t.checkResponse(res, 204);
            _cb(null, res.headers);
        });
    };
    this.client.mkdir(this.dir, cb);

    this.checkContent = function checkContent(opts) {
        var t = opts.t;
        var stream = opts.stream;
        var res = opts.res;
        t.ok(stream);
        t.ok(res);
        t.checkResponse(res, opts.code);

        stream.setEncoding('utf8');
        var body = '';
        stream.on('data', function (chunk) {
            body += chunk;
        });
        stream.once('error', function (err) {
            t.ifError(err);
            t.end();
        });
        stream.once('end', function () {
            t.equal(body, opts.text);
            t.end();
        });

        stream.resume();
    };

    this.checkDefaultContent = function checkDefaultContent(t,
                                                            stream,
                                                            res) {
        self.checkContent({
            t: t,
            stream: stream,
            res: res,
            code: 200,
            text: TEXT
        });
        t.equal(res.headers['content-type'], 'text/plain');
        t.ok(res.headers.etag);
        t.ok(res.headers['last-modified']);
    };
});


after(function (cb) {
    this.client.rmr(this.dir, cb.bind(null, null));
});


test('put object', function (t) {
    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';
    var size = Buffer.byteLength(text);
    process.nextTick(stream.end.bind(stream, text));

    this.client.put(this.key, stream, {size: size}, function (err, res) {
        t.ifError(err);
        t.checkResponse(res, 204);
        t.end();
    });
});


test('put overwrite', function (t) {
    var etag;
    var self = this;
    this.putObject(t, function (_, headers) {
        etag = headers.etag;
        self.putObject(t, function (__, headers2) {
            t.ok(etag !== headers2.etag);
            t.end();
        });
    });
});


test('put/get zero bytes', function (t) {
    var self = this;
    var stream = new MemoryStream();
    process.nextTick(stream.end.bind(stream));

    this.client.put(this.key, stream, {size: 0}, function (err, res) {
        t.ifError(err);
        if (err) {
            t.end();
            return;
        }
        t.checkResponse(res, 204);
        self.client.get(self.key, function (err2, stream2, res2) {
            t.ifError(err2);
            t.checkResponse(res2, 200);
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
    var self = this;
    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';
    var size = Buffer.byteLength(text);

    process.nextTick(stream.end.bind(stream, text));
    this.client.put(this.key, stream, function (err, res) {
        t.ifError(err);
        t.checkResponse(res, 204);
        self.client.get(self.key, function (err2, stream2, res2) {
            t.ifError(err2);
            t.checkResponse(res2, 200);
            t.equal(res2.headers['content-length'], size);
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
    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';

    process.nextTick(stream.end.bind(stream, text));
    var opts = {
        headers: {
            'max-content-length': 1
        }
    };
    this.client.put(this.key, stream, opts, function (err, res) {
        t.ok(err);
        t.ok(res);
        t.equal(err.name, 'MaxContentLengthExceededError');
        t.checkResponse(res, 413);
        t.end();
    });
});


test('put object (1 copy)', function (t) {
    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';
    var size = Buffer.byteLength(text);
    var _opts = {
        copies: 1,
        size: size
    };
    process.nextTick(stream.end.bind(stream, text));

    this.client.put(this.key, stream, _opts, function (err, res) {
        t.ifError(err);
        t.checkResponse(res, 204);
        t.end();
    });
});



test('put object (3 copies)', function (t) {
    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';
    var size = Buffer.byteLength(text);
    var _opts = {
        copies: 3,
        size: size
    };
    process.nextTick(stream.end.bind(stream, text));

    // This fails against COAL
    this.client.put(this.key, stream, _opts, function (err, res) {
        if (err) {
            t.equal(err.name, 'NotEnoughSpaceError');
            t.checkResponse(res, 507);
        } else {
            t.checkResponse(res, 204);
        }
        t.end();
    });
});


// Default maximum is 9 copies.
test('put object (10 copies)', function (t) {
    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';
    var size = Buffer.byteLength(text);
    var _opts = {
        copies: 10,
        size: size
    };
    process.nextTick(stream.end.bind(stream, text));

    this.client.put(this.key, stream, _opts, function (err, res) {
        t.ok(err);
        if (err)
            t.equal(err.name, 'InvalidDurabilityLevelError');
        t.checkResponse(res, 400);
        t.end();
    });
});


test('chattr: m- headers', function (t) {
    var opts = {
        headers: {
            'm-foo': 'bar',
            'm-bar': 'baz'
        }
    };
    var self = this;

    this.putObject(t, function () {
        self.client.chattr(self.key, opts, function (err) {
            t.ifError(err);

            self.client.info(self.key, function (err2, info) {
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
    var opts = {
        headers: {
            'content-type': 'jpg'
        }
    };
    var self = this;

    this.putObject(t, function () {
        self.client.chattr(self.key, opts, function (err) {
            t.ifError(err);
            t.ifError(err);

            self.client.info(self.key, function (err2, info) {
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


test('chattr: durability-level (not ok)', function (t) {
    var opts = {
        headers: {
            'durability-level': '4'
        }
    };
    var self = this;

    this.putObject(t, function () {
        self.client.chattr(self.key, opts, function (err) {
            t.ok(err);
            t.equal(err.name, 'InvalidUpdateError');
            t.end();
        });
    });
});


test('chattr: content-md5 (not ok)', function (t) {
    var opts = {
        headers: {
            'content-md5': 'foo'
        }
    };
    var self = this;

    this.putObject(t, function () {
        self.client.chattr(self.key, opts, function (err) {
            t.ok(err);
            t.equal(err.name, 'InvalidUpdateError');
            t.end();
        });
    });
});


test('chattr: content-length (not ok)', function (t) {
    var opts = {
        headers: {
            'content-length': '4'
        }
    };
    var self = this;

    this.putObject(t, function () {
        self.client.chattr(self.key, opts, function (err) {
            t.ok(err);
            t.equal(err.name, 'InvalidUpdateError');
            t.end();
        });
    });
});


test('chattr: no object', function (t) {
    var opts = {
        headers: {
            'm-foo': 'bar'
        }
    };

    this.client.chattr(this.key, opts, function (err) {
        t.ok(err);
        t.equal(err.name, 'ResourceNotFoundError');
        t.end();
    });
});



test('MANTA-625 (custom headers)', function (t) {
    var opts = {
        headers: {
            'm-foo': 'bar'
        }
    };
    var self = this;

    this.putObject(t, opts, function () {
        self.client.get(self.key, function (err, stream, res) {
            t.ifError(err);
            t.checkResponse(res, 200);
            t.equal(res.headers['m-foo'], 'bar');
            stream.once('end', t.end.bind(t));
            stream.resume();
        });
    });
});


test('put if-match ok', function (t) {
    var etag;
    var self = this;
    this.putObject(t, function (_, headers) {
        etag = headers.etag;
        var opts = {
            headers: {
                'if-match': etag
            }
        };
        self.putObject(t, opts, function (__, headers2) {
            t.ok(etag !== headers2.etag);
            t.end();
        });
    });
});


test('put if-match fail', function (t) {
    var self = this;
    this.putObject(t, function () {
        var opts = {
            headers: {
                'if-match': uuid.v4()
            }
        };
        writeObject(self.client, self.key, opts, function (err, res) {
            t.ok(err);
            t.ok(res);
            t.equal(err.name, 'PreconditionFailedError');
            t.checkResponse(res, 412);
            t.end();
        });
    });
});


test('put if-none-match ok', function (t) {
    var etag;
    var self = this;
    this.putObject(t, function (_, headers) {
        etag = headers.etag;
        var opts = {
            headers: {
                'if-none-match': uuid.v4()
            }
        };
        self.putObject(t, opts, function (__, headers2) {
            t.ok(etag !== headers2.etag);
            t.end();
        });
    });
});


test('put if-none-match fail', function (t) {
    var etag;
    var self = this;
    this.putObject(t, function (_, headers) {
        etag = headers.etag;
        var opts = {
            headers: {
                'if-none-match': etag
            }
        };
        writeObject(self.client, self.key, opts, function (err, res) {
            t.ok(err);
            t.ok(res);
            t.equal(err.name, 'PreconditionFailedError');
            t.checkResponse(res, 412);
            t.end();
        });
    });
});


test('put unmodified-since ok', function (t) {
    var self = this;
    this.putObject(t, function (_, headers) {
        var date = headers['last-modified'];
        var etag = headers.etag;
        var opts = {
            headers: {
                'if-unmodified-since': date
            }
        };
        self.putObject(t, opts, function (__, headers2) {
            t.ok(etag !== headers2.etag);
            t.end();
        });
    });
});


test('put unmodified-since fail', function (t) {
    var self = this;
    this.putObject(t, function (_, headers) {
        var d = new Date(Date.parse(headers['last-modified']) - 10000);
        var old = restify.httpDate(new Date(d));
        var opts = {
            headers: {
                'if-unmodified-since': old
            }
        };
        writeObject(self.client, self.key, opts, function (err, res) {
            t.ok(err);
            t.ok(res);
            t.equal(err.name, 'PreconditionFailedError');
            t.checkResponse(res, 412);
            t.end();
        });
    });
});


test('put bad content-md5', function (t) {
    var opts = {
        md5: 'bogus',
        size: Buffer.byteLength(TEXT),
        type: 'text/plain'
    };
    var stream = new MemoryStream();

    this.client.put(this.key, stream, opts, function (err, res) {
        t.ok(err);
        t.ok(res);
        t.equal(err.name, 'BadRequestError');
        t.checkResponse(res, 400);
        t.end();
    });
    process.nextTick(stream.end.bind(stream, TEXT));
});


test('put parent ENOEXIST', function (t) {
    var k = this.root + '/' + uuid.v4() + '/' + uuid.v4();
    writeObject(this.client, k, function (err, res) {
        t.ok(err);
        t.ok(res);
        t.equal(err.name, 'DirectoryDoesNotExistError');
        t.checkResponse(res, 404);
        t.end();
    });
});


test('put parent not directory', function (t) {
    var self = this;
    this.putObject(t, function () {
        var k = self.key + '/' + uuid.v4();
        writeObject(self.client, k, function (err, res) {
            t.ok(err);
            t.ok(res);
            t.equal(err.name, 'ParentNotDirectoryError');
            t.checkResponse(res, 400);
            t.end();
        });
    });
});


test('put too big', function (t) {
    // Since MANTA-2510 we use multiDC=false and ignoreSize=true config for
    // COAL. Therefore, this test will never throw a NotEnoughSpaceError but
    // instead an UploadTimeoutError. Avoid running it if we find we're
    // running in COAL:
    if (process.env.SDC_URL && process.env.SDC_URL.indexOf('coal') !== -1) {
        t.end();
        return;
    }

    var opts = {
        size: 1000000000000000,
        type: 'text/plain'
    };
    var stream = new MemoryStream();

    this.client.put(this.key, stream, opts, function (err, res) {
        t.ok(err);
        t.equal(err.name, 'NotEnoughSpaceError');
        t.checkResponse(res, 507);
        t.end();
    });
});


test('get ok', function (t) {
    var self = this;
    this.putObject(t, function () {
        self.client.get(self.key, function (err, stream, res) {
            t.ifError(err);
            self.checkDefaultContent(t, stream, res);
            t.equal('bytes', res.headers['accept-ranges']);
            t.end();
        });
    });
});


test('get 404', function (t) {
    this.client.get(this.key + 'a', function (err, stream, res) {
        t.ok(err);
        t.equal(err.name, 'ResourceNotFoundError');
        t.checkResponse(res, 404);
        t.end();
    });
});


test('get 406', function (t) {
    var self = this;
    this.putObject(t, function () {
        var opts = {
            accept: 'application/json'
        };
        self.client.get(self.key, opts, function (err, stream, res) {
            t.ok(err);
            t.equal(err.name, 'NotAcceptableError');
            t.checkResponse(res, 406);
            t.end();
        });
    });
});


test('get if-match ok', function (t) {
    var self = this;
    this.putObject(t, function (_, headers) {
        var opts = {
            headers: {
                'if-match': headers.etag
            }
        };
        self.client.get(self.key, opts, function (err, stream, res) {
            t.ifError(err);
            t.checkResponse(res, 200);
            t.end();
        });
    });
});


test('get if-match fail', function (t) {
    var self = this;
    this.putObject(t, function () {
        var opts = {
            headers: {
                'if-match': uuid.v4()
            }
        };
        self.client.get(self.key, opts, function (err, stream, res) {
            t.ok(err);
            t.equal(err.name, 'PreconditionFailedError');
            t.equal(stream, null);
            t.checkResponse(res, 412);
            t.end();
        });
    });
});


test('get 304', function (t) {
    var self = this;
    this.putObject(t, function (_, headers) {
        var opts = {
            headers: {
                'if-none-match': headers.etag
            }
        };
        self.client.get(self.key, opts, function (err, stream, res) {
            t.ifError(err);
            t.checkResponse(res, 304);
            t.equal(stream, null);
            t.end();
        });
    });
});


test('get if-none-match ok', function (t) {
    var self = this;
    this.putObject(t, function () {
        var opts = {
            headers: {
                'if-none-match': uuid.v4()
            }
        };
        self.client.get(self.key, opts, function (err, stream, res) {
            t.ifError(err);
            t.checkResponse(res, 200);
            t.end();
        });
    });
});


test('get if-modified-since ok (data)', function (t) {
    var self = this;
    this.putObject(t, function () {
        var d = restify.httpDate(new Date(1));
        var opts = {
            headers: {
                'if-modified-since': d
            }
        };
        self.client.get(self.key, opts, function (err, stream, res) {
            t.ifError(err);
            t.checkResponse(res, 200);
            t.end();
        });
    });
});


test('get if-modified-since 304', function (t) {
    var self = this;
    this.putObject(t, function () {
        var d = restify.httpDate(new Date(Date.now() + 10000));
        var opts = {
            headers: {
                'if-modified-since': d
            }
        };
        self.client.get(self.key, opts, function (err, stream, res) {
            t.ifError(err);
            t.checkResponse(res, 304);
            t.end();
        });
    });
});


test('get if-unmodified-since 200', function (t) {
    var self = this;
    this.putObject(t, function (_, headers) {
        var opts = {
            headers: {
                'if-unmodified-since': headers['last-modified']
            }
        };
        self.client.get(self.key, opts, function (err, stream, res) {
            t.ifError(err);
            t.checkResponse(res, 200);
            t.end();
        });
    });
});


test('get if-unmodified-since 412', function (t) {
    var self = this;
    this.putObject(t, function (_, headers) {
        var d = restify.httpDate(new Date(Date.now() - 100000));
        var opts = {
            headers: {
                'if-unmodified-since': d
            }
        };
        self.client.get(self.key, opts, function (err, stream, res) {
            t.ok(err);
            t.equal(err.name, 'PreconditionFailedError');
            t.checkResponse(res, 412);
            t.end();
        });
    });
});


test('get range', function (t) {
    var self = this;
    var stream = new MemoryStream();
    var text = 'abcdefghijklmnopqrstuvwxyz';
    var size = Buffer.byteLength(text);
    process.nextTick(stream.end.bind(stream, text));

    this.client.put(this.key, stream, {size: size}, function (err, res) {
        var opts = {
            headers: {
                'range': 'bytes=3-8'
            }
        };
        self.client.get(self.key, opts, function (err2, s, r) {
            self.checkContent({
                t: t,
                stream: s,
                res: r,
                code: 206,
                text: 'defghi'
            });
            t.equal(undefined, r.headers['accept-ranges']);
            t.equal(undefined, r.headers['content-md5']);
            t.equal(6, r.headers['content-length']);
            t.ok(r.headers.etag);
            t.equal('application/octet-stream',
                    r.headers['content-type']);
            t.equal('bytes 3-8/26', r.headers['content-range']);
        });
    });
});


test('get range, prefix', function (t) {
    var self = this;
    var stream = new MemoryStream();
    var text = 'abcdefghijklmnopqrstuvwxyz';
    var size = Buffer.byteLength(text);
    process.nextTick(stream.end.bind(stream, text));

    this.client.put(this.key, stream, {size: size}, function (err, res) {
        var opts = {
            headers: {
                'range': 'bytes=19-'
            }
        };
        self.client.get(self.key, opts, function (err2, s, r) {
            self.checkContent({
                t: t,
                stream: s,
                res: r,
                code: 206,
                text: 'tuvwxyz'
            });
            t.equal(7, r.headers['content-length']);
            t.equal('bytes 19-25/26', r.headers['content-range']);
        });
    });
});


test('get range, suffix', function (t) {
    var self = this;
    var stream = new MemoryStream();
    var text = 'abcdefghijklmnopqrstuvwxyz';
    var size = Buffer.byteLength(text);
    process.nextTick(stream.end.bind(stream, text));

    this.client.put(this.key, stream, {size: size}, function (err, res) {
        var opts = {
            headers: {
                'range': 'bytes=-10'
            }
        };
        self.client.get(self.key, opts, function (err2, s, r) {
            self.checkContent({
                t: t,
                stream: s,
                res: r,
                code: 206,
                text: 'qrstuvwxyz'
            });
            t.equal(10, r.headers['content-length']);
            t.equal('bytes 16-25/26', r.headers['content-range']);
        });
    });
});


test('get range, multi-range', function (t) {
    var self = this;
    var stream = new MemoryStream();
    var text = 'abcdefghijklmnopqrstuvwxyz';
    var size = Buffer.byteLength(text);
    process.nextTick(stream.end.bind(stream, text));

    this.client.put(this.key, stream, {size: size}, function (err, res) {
        var opts = {
            headers: {
                'range': 'bytes=0-5,6-10'
            }
        };
        self.client.get(self.key, opts, function (err2, s, r) {
            t.equal(501, r.statusCode);
            t.end();
        });
    });
});


test('get invalid range', function (t) {
    var self = this;
    var stream = new MemoryStream();
    var text = 'abcdefghijklmnopqrstuvwxyz';
    var size = Buffer.byteLength(text);
    process.nextTick(stream.end.bind(stream, text));

    this.client.put(this.key, stream, {size: size}, function (err, res) {
        var opts = {
            headers: {
                'range': 'bytes=foo'
            }
        };
        self.client.get(self.key, opts, function (err2, s, r) {
            t.equal(416, r.statusCode);
            t.equal('bytes */26', r.headers['content-range']);
            t.end();
        });
    });
});


test('get range, out of bounds', function (t) {
    var self = this;
    var stream = new MemoryStream();
    var text = 'abcdefghijklmnopqrstuvwxyz';
    var size = Buffer.byteLength(text);
    process.nextTick(stream.end.bind(stream, text));

    this.client.put(this.key, stream, {size: size}, function (err, res) {
        var opts = {
            headers: {
                'range': 'bytes=27-100'
            }
        };
        self.client.get(self.key, opts, function (err2, s, r) {
            t.equal(416, r.statusCode);
            t.equal('bytes */26', r.headers['content-range']);
            t.end();
        });
    });
});


test('del ok', function (t) {
    var self = this;
    this.putObject(t, function () {
        self.client.unlink(self.key, function (err, res) {
            t.ifError(err);
            t.checkResponse(res, 204);
            t.end();
        });
    });
});


test('del 404', function (t) {
    this.client.unlink(this.key + 'a', function (err, res) {
        t.ok(err);
        t.equal(err.name, 'ResourceNotFoundError');
        t.checkResponse(res, 404);
        t.end();
    });
});


test('del if-match ok', function (t) {
    var self = this;
    this.putObject(t, function (_, headers) {
        var etag = headers.etag;
        var opts = {
            headers: {
                'if-match': etag
            }
        };
        self.client.unlink(self.key, opts, function (err, res) {
            t.ifError(err);
            t.checkResponse(res, 204);
            t.end();
        });
    });
});


test('del if-match fail', function (t) {
    var self = this;
    this.putObject(t, function () {
        var opts = {
            headers: {
                'if-match': uuid.v4()
            }
        };
        self.client.unlink(self.key, opts, function (err, res) {
            t.ok(err);
            t.equal(err.name, 'PreconditionFailedError');
            t.checkResponse(res, 412);
            t.end();
        });
    });
});


test('del if-none-match ok', function (t) {
    var self = this;
    this.putObject(t, function () {
        var opts = {
            headers: {
                'if-none-match': uuid.v4()
            }
        };
        self.client.unlink(self.key, opts, function (err, res) {
            t.ifError(err);
            t.checkResponse(res, 204);
            t.end();
        });
    });
});


test('del if-none-match fail', function (t) {
    var self = this;
    this.putObject(t, function (_, headers) {
        var opts = {
            headers: {
                'if-none-match': headers.etag
            }
        };
        self.client.unlink(self.key, opts, function (err, res) {
            t.ok(err);
            t.equal(err.name, 'PreconditionFailedError');
            t.checkResponse(res, 412);
            t.end();
        });
    });
});


test('put timeout', function (t) {
    var opts = {
        size: Buffer.byteLength(TEXT),
        type: 'text/plain'
    };
    var stream = new MemoryStream();

    this.client.put(this.key, stream, opts, function (err, res) {
        t.ok(err);
        t.equal(err.name, 'UploadTimeoutError');
        t.end();
    });

    process.nextTick(function () {
        stream.write(TEXT.substr(0, 1));
    });
});
