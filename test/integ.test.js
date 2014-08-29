/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
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
    this.client = helper.createClient();
    this.root = '/' + this.client.account + '/stor';
    this.dir = this.root + '/' + uuid.v4();
    this.key = this.dir + '/' + uuid.v4();
    this.client.mkdir(this.dir, cb);
});


after(function (cb) {
    this.client.rmr(this.dir, cb.bind(null, null));
});


test('MANTA-796: URL encoding', function (t) {
    var k = this.dir + '/foo\'b';
    this.client.mkdir(k, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('MANTA-646', function (t) {
    var self = this;
    var runs = 0;

    function run() {
        var j = 0;

        function cb(err) {
            t.ifError(err);
            if (++j === 5) {
                if (++runs < 10) {
                    run();
                } else {
                    t.end();
                }
            }
        }

        for (var i = 0; i < 5; i++)
            self.client.mkdir(self.key, cb);
    }

    run();
});


test('MANTA-646: mkdirp retry', function (t) {
    var self = this;
    var runs = 0;

    function run() {
        var j = 0;

        function cb(err) {
            t.ifError(err);

            if (++j === 10) {
                if (++runs < 10) {
                    run();
                } else {
                    t.end();
                }
            }
        }

        for (var i = 0; i < 10; i++)
            self.client.mkdirp(self.key, cb);
    }

    run();
});


// We want to be sure that we're eating etag conflicts IFF the client
// sends no etag but that we do return ConcurrentRequestError when there
// are conflicts and they did send an etag
test('MANTA-1072 no etag', function (t) {
    var self = this;
    var runs = 0;

    function run() {
        var j = 0;

        function cb(err) {
            t.ifError(err);
            if (++j === 3) {
                if (++runs < 10) {
                    process.nextTick(run);
                } else {
                    t.end();
                }
            }
        }

        for (var i = 0; i < 3; i++)
            writeObject(self.client, self.key, cb);
    }

    run();
});


test('MANTA-1072 conditional', function (t) {
    var sawError = false;
    var self = this;
    var runs = 0;

    function run(etag) {
        var j = 0;

        var opts = {
            headers: {
                'if-match': etag
            }
        };

        function cb(err, res) {
            if (err) {
                t.ok(err.name === 'ConcurrentRequestError' ||
                     err.name === 'PreconditionFailedError');
                sawError = true;
            }

            if (++j === 3) {
                if (++runs < 10) {
                    process.nextTick(function () {
                        run(res.headers['etag']);
                    });
                } else {
                    t.ok(sawError);
                    t.end();
                }
            }
        }

        for (var i = 0; i < 3; i++)
            writeObject(self.client, self.key, opts, cb);
    }

    writeObject(this.client, this.key, function (err, res) {
        t.ifError(err);
        t.ok(res);
        t.ok(((res || {}).headers || {}).etag);
        if (err || !res || !res.headers['etag']) {
            t.end();
            return;
        }

        run(res.headers['etag']);
    });
});

// Depends on devs.ldif being loaded
// test('MANTA-792: email as login', function (t) {
//         this.client.ls('/josh@wilsdon.ca/public', function (err, res) {
//                 t.ifError(err);
//                 res.on('end', function () {
//                         t.end();
//                 });
//         });
// });


test('MANTA-796: URL encode directory', function (t) {
    var k = this.dir + '/foo\'b';
    var self = this;
    this.client.mkdir(k, function (err) {
        t.ifError(err);
        self.client.ls(k, function (err2, res) {
            t.ifError(err2);
            res.once('end', t.end.bind(t));
        });
    });
});


test('MANTA-1593: URL encode object', function (t) {
    var k = this.dir + '/fo o\'b';
    var opts = {
        size: Buffer.byteLength(TEXT),
        type: 'text/plain'
    };
    var self = this;
    var stream = new MemoryStream();
    this.client.put(k, stream, opts, function (err) {
        t.ifError(err);
        self.client.get(k, function (err2, stream2) {
            t.ifError(err2);
            stream2.once('end', t.end.bind(t));
            stream2.resume();
        });
    });
    process.nextTick(stream.end.bind(stream, TEXT));
});


test('MANTA-1593: URL encoded listing', function (t) {
    var opts = {
        size: Buffer.byteLength(TEXT),
        type: 'text/plain'
    };
    var self = this;
    var stream = new MemoryStream();
    var k = this.dir + '/my file.txt';
    this.client.put(k, stream, opts, function (err) {
        t.ifError(err);
        self.client.ls(self.dir, function (err2, res) {
            t.ifError(err2);

            if (err2) {
                t.end();
                return;
            }

            var found = false;
            res.on('object', function (o) {
                t.ok(o);
                t.equal(o.name, 'my file.txt');
                found = true;
            });

            res.once('error', function (err3) {
                t.ifError(err3);
                t.end();
            });

            res.once('end', function () {
                t.ok(found);
                t.end();
            });
        });
    });
    process.nextTick(stream.end.bind(stream, TEXT));
});


test('MANTA-1593: URL encoded link', function (t) {
    var opts = {
        size: Buffer.byteLength(TEXT),
        type: 'text/plain'
    };
    var self = this;
    var stream = new MemoryStream();
    var k = this.dir + '/my file.txt';
    var k2 = this.dir + '/my file 2.txt';
    this.client.put(k, stream, opts, function (err) {
        t.ifError(err);
        self.client.ln(k, k2, function (err2) {
            t.ifError(err2);
            t.end();
        });
    });
    process.nextTick(stream.end.bind(stream, TEXT));
});


// Requires x-dc setup
// test('MANTA-1853: ENOSPACE on durability-level=6', function (t) {
//     var k = this.dir + '/6copies.txt';
//     var opts = {
//         copies: 6,
//         size: Buffer.byteLength(TEXT),
//         type: 'text/plain'
//     };
//     var stream = new MemoryStream();

//     this.client.put(k, stream, opts, function (err) {
//         t.ifError(err);
//         t.end();
//     });

//     process.nextTick(stream.end.bind(stream, TEXT));
// });
