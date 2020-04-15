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
    this.pubRoot = '/' + this.client.user + '/public';
    this.root = '/' + this.client.user + '/stor';
    this.dir = this.root + '/' + uuidv4();
    this.pubKey = this.pubRoot + '/' + uuidv4();
    this.key = this.dir + '/' + uuidv4();
    this.obj = this.dir + '/' + uuidv4();

    this.checkContent = function checkContent(t, stream, res) {
        t.ok(stream);
        t.ok(res);
        t.checkResponse(res, 200);
        t.equal(res.headers['content-type'], 'text/plain');
        t.ok(res.headers.etag);
        t.ok(res.headers['last-modified']);

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
            t.equal(body, TEXT);
            t.end();
        });
    };

    this.client.mkdir(this.dir, function (err) {
        if (err) {
            cb(err);
            return;
        }
        writeObject(self.client, self.obj, function (err2, res) {
            if (err2) {
                cb(err2);
                return;
            }

            self.etag = res.headers.etag;
            self.mtime = res.headers['last-modified'];

            // Set up a public object
            writeObject(self.client, self.pubKey, function (err3) {
                if (err3) {
                    cb(err3);
                    return;
                }

                cb();
            });
        });
    });
});


after(function (cb) {
    this.client.rmr(this.dir, cb.bind(null, null));
});


test('put link', function (t) {
    this.client.ln(this.obj, this.key, function (err, res) {
        t.ok(err && err.code === 'SnaplinksDisabledError',
            'Expected SnapLinks to be disabled');
        t.checkResponse(res, 403);
        t.end();
    });
});


// Requires an object /admin/stor. Otherwise, duplicates "put source not found"
// test('put source account not authorized', function (t) {
//     var self = this;
//     var k = '/admin/stor/muskie_link_unit_test';
//     /* JSSTYLED */
//     var re = /^.+ is not allowed to access \/admin\/stor\/muskie_link_tests/;
//
//     self.client.ln(k, self.key, function (err2, res2) {
//         t.ok(err2);
//         t.equal(err2.name, 'AuthorizationFailedError');
//         t.ok(re.test(err2.message));
//         t.checkResponse(res2, 403);
//         t.end();
//     });
// });


// Requires an admin user
// test('put source account not found', function (t) {
//         var src = '/' + uuidv4().substr(0, 7) + '/stor/' + uuidv4();
//         this.client.ln(src, this.key, function (err, res) {
//                 t.ok(err);
//                 t.equal(err.name, 'SourceObjectNotFoundError');
//                 t.checkResponse(res, 404);
//                 t.end();
//         });
// });
