/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

// Test various other tests, typically regression tests for specific tickets
// that don't fit in a full separate test file.

var crypto = require('crypto');

var MemoryStream = require('stream').PassThrough;
var restify = require('restify');
var test = require('tap').test;
var uuidv4 = require('uuid/v4');
var vasync = require('vasync');

var helper = require('../helper');


///--- Globals

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

var client;
var dir;
var key;
var testAccount;

test('setup: test account', function (t) {
    helper.ensureTestAccounts(t, function (err, accounts) {
        t.ifError(err, 'no error loading/creating test accounts');
        t.ok(accounts.regular, 'have regular test account: ' +
            accounts.regular.login);
        testAccount = accounts.regular;
        t.end();
    });
});

test('setup: client and test dir', function (t) {
    client = helper.mantaClientFromAccountInfo(testAccount);
    var root = '/' + client.user + '/stor';
    dir = root + '/test-various-dir-' + uuidv4().split('-')[0];
    key = dir + '/test-various-file-' + uuidv4().split('-')[0];

    client.mkdir(dir, function (err) {
        t.ifError(err, 'make test dir ' + dir);
        t.end();
    });
});


test('MANTA-796: URL encoding', function (t) {
    var k = dir + '/foo\'b';
    client.mkdir(k, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('MANTA-646: mkdir race', function (t) {
    var runs = 0;
    var testDir = dir + '/mkdir-race';

    function run() {
        var j = 0;

        function cb(err) {
            t.ifError(err, 'j=' + j + ', run ' + runs + ', no mkdir error');
            if (++j === 5) {
                if (++runs < 10) {
                    run();
                } else {
                    t.end();
                }
            }
        }

        for (var i = 0; i < 5; i++) {
            client.mkdir(testDir, cb);
        }
    }

    run();
});


test('MANTA-646: mkdirp race', function (t) {
    var runs = 0;
    var testDir = dir + '/mkdirp-race';

    function run() {
        var j = 0;

        function cb(err) {
            t.ifError(err,
                'j=' + j + ', run ' + runs + ', no mkdirp error');
            if (++j === 10) {
                if (++runs < 10) {
                    run();
                } else {
                    t.end();
                }
            }
        }

        for (var i = 0; i < 10; i++) {
            client.mkdirp(testDir, cb);
        }
    }

    run();
});


// We want to be sure that we're eating etag conflicts IFF the client
// sends no etag but that we do return ConcurrentRequestError when there
// are conflicts and they did send an etag
test('MANTA-1072 no etag', function (t) {
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

        for (var i = 0; i < 3; i++) {
            writeObject(client, key, cb);
        }
    }

    run();
});


test('MANTA-1072 conditional', function (t) {
    var sawError = false;
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
                    err.name === 'PreconditionFailedError',
                    'err is ConcurrentRequestError or ' +
                    'PreconditionFailedError: err.name=' + err.name);
                sawError = true;
            }

            if (++j === 3) {
                if (++runs < 10) {
                    setImmediate(run, res.headers['etag']);
                } else {
                    t.ok(sawError, 'saw ConcurrentRequestError or ' +
                        'PreconditionFailedError at least once');
                    t.end();
                }
            }
        }

        for (var i = 0; i < 3; i++) {
            writeObject(client, key, opts, cb);
        }
    }

    writeObject(client, key, function (err, res) {
        t.ifError(err, 'no error writing ' + key);
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
//         client.ls('/josh@wilsdon.ca/public', function (err, res) {
//                 t.ifError(err);
//                 res.on('end', function () {
//                         t.end();
//                 });
//         });
// });


test('MANTA-796: URL encode directory', function (t) {
    var testDir = dir + '/foo\'b';
    client.mkdir(testDir, function (err) {
        t.ifError(err, 'no error creating dir "' + testDir + '"');
        client.ls(testDir, function (lsErr, res) {
            t.ifError(lsErr, 'no error mls\'ing dir');
            res.once('end', function onEnd() {
                t.end();
            });
        });
    });
});


test('MANTA-1593: URL encode object', function (t) {
    var testKey = dir + '/fo o\'b';
    var opts = {
        size: Buffer.byteLength(TEXT),
        type: 'text/plain'
    };
    var stream = new MemoryStream();
    client.put(testKey, stream, opts, function (err) {
        t.ifError(err, 'no error putting object "' + testKey + '"');
        client.get(testKey, function (getErr, getStream) {
            t.ifError(getErr, 'no error getting object');
            getStream.once('end', function onEnd() {
                t.end();
            });
            getStream.resume();
        });
    });
    setImmediate(function endIt() {
        stream.end(TEXT);
    });
});


test('MANTA-1593: URL encoded listing', function (t) {
    var opts = {
        size: Buffer.byteLength(TEXT),
        type: 'text/plain'
    };
    var stream = new MemoryStream();
    var testName = 'my file.txt';
    var testKey = dir + '/' + testName;
    client.put(testKey, stream, opts, function (err) {
        t.ifError(err, 'no error putting test file "' + testKey + '"');
        client.ls(dir, function (lsErr, res) {
            t.ifError(lsErr, 'no mls error');

            if (lsErr) {
                t.end();
                return;
            }

            var found = false;
            res.on('object', function (o) {
                if (o.name === testName) {
                    found = true;
                }
            });
            res.once('error', function (resErr) {
                t.ifError(resErr, 'no "error" event on mls stream');
                t.end();
            });
            res.once('end', function () {
                t.ok(found,
                    'found "' + testKey + '" test file in dir listing');
                t.end();
            });
        });
    });
    process.nextTick(stream.end.bind(stream, TEXT));
});


// Requires x-dc setup
// test('MANTA-1853: ENOSPACE on durability-level=6', function (t) {
//     var k = dir + '/6copies.txt';
//     var opts = {
//         copies: 6,
//         size: Buffer.byteLength(TEXT),
//         type: 'text/plain'
//     };
//     var stream = new MemoryStream();
//     client.put(k, stream, opts, function (err) {
//         t.ifError(err);
//         t.end();
//     });
//     process.nextTick(stream.end.bind(stream, TEXT));
// });


test('teardown', function (t) {
    client.rmr(dir, function onRm(err) {
        t.ifError(err, 'remove test dir ' + dir);
        t.end();
    });
});
