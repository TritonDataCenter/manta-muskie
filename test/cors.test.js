/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var crypto = require('crypto');

var MemoryStream = require('stream').PassThrough;
var uuidv4 = require('uuid/v4');

if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');



///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;

var TEXT = 'hello world';



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
    this.httpClient = helper.createRawClient();
    this.root = '/' + this.client.user + '/stor';
    this.dir = this.root + '/' + uuidv4();
    this.key = this.dir + '/' + uuidv4();

    this.get = function get(t, headers, _cb) {
        var opts = {
            headers: headers
        };
        self.client.get(self.key, opts, function (err, stream, res) {
            t.ifError(err);
            if (err) {
                t.end();
                return;
            }

            var body = '';
            stream.setEncoding('utf8');
            stream.on('data', function (chunk) {
                body += chunk;
            });
            stream.once('error', function (err2) {
                t.ifError(err2);
                t.end();
            });
            stream.once('end', function () {
                t.equal(body, TEXT);
                t.ok(res);
                t.equal(res.statusCode, 200);

                var hdrs = res.headers;
                if (_cb) {
                    _cb(hdrs);
                    return;
                }
                t.equal(hdrs['access-control-allow-origin'],
                        headers.origin);
                t.end();
            });
        });
    };

    this.put = function put(headers, _cb) {
        var md5 = crypto.createHash('md5');
        var opts = {
            headers: headers,
            md5: md5.update(TEXT).digest('base64'),
            size: Buffer.byteLength(TEXT),
            type: 'text/plain'
        };
        var stream = new MemoryStream();

        self.client.put(self.key, stream, opts, _cb);
        process.nextTick(stream.end.bind(stream, TEXT));
    };

    this.client.mkdir(this.dir, function (err) {
        if (err) {
            cb(err);
            return;
        }
        cb();
    });
});


after(function (cb) {
    this.client.rmr(this.dir, cb.bind(null, null));
});


test('origin *', function (t) {
    var put_headers = {
        'access-control-allow-origin': '*'
    };
    var self = this;

    this.put(put_headers, function (put_err) {
        t.ifError(put_err);
        var headers = {
            origin: 'http://127.0.0.1'
        };
        self.get(t, headers);
    });
});


test('origin list', function (t) {
    var put_headers = {
        'access-control-allow-origin': 'http://foo.com, http://bar.com'
    };
    var self = this;

    this.put(put_headers, function (put_err) {
        t.ifError(put_err);
        var headers = {
            origin: 'http://foo.com'
        };
        self.get(t, headers);
    });
});


test('origin deny', function (t) {
    var put_headers = {
        'access-control-allow-origin': 'http://foo.com'
    };
    var self = this;

    this.put(put_headers, function (put_err) {
        t.ifError(put_err);
        var headers = {
            origin: 'http://bar.com'
        };
        self.get(t, headers, function (h) {
            t.notOk(h['access-control-allow-origin']);
            t.end();
        });
    });
});


test('method explicit', function (t) {
    var put_headers = {
        'access-control-allow-origin': '*',
        'access-control-allow-methods': 'GET'
    };
    var self = this;

    this.put(put_headers, function (put_err) {
        t.ifError(put_err);
        var headers = {
            origin: 'http://foo.com'
        };
        self.get(t, headers);
    });
});


test('method explicit fail', function (t) {
    var put_headers = {
        'access-control-allow-origin': '*',
        'access-control-allow-methods': 'DELETE'
    };
    var self = this;

    this.put(put_headers, function (put_err) {
        t.ifError(put_err);
        var headers = {
            origin: 'http://foo.com'
        };
        self.get(t, headers, function (h) {
            t.notOk(h['access-control-allow-origin']);
            t.notOk(h['access-control-allow-methods']);
            t.end();
        });
    });
});


test('other access-control ok', function (t) {
    var put_headers = {
        'access-control-allow-origin': '*',
        'access-control-expose-headers': 'x-foo',
        'access-control-max-age': 3600
    };
    var self = this;

    this.put(put_headers, function (put_err) {
        t.ifError(put_err);
        var headers = {
            origin: 'http://foo.com'
        };
        self.get(t, headers, function (h) {
            t.equal(h['access-control-allow-origin'],
                    headers.origin);
            t.equal(h['access-control-expose-headers'],
                    put_headers['access-control-expose-headers']);
            t.notOk(h['access-control-max-age']);
            t.end();
        });
    });
});
