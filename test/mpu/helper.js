/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var crypto = require('crypto');
var manta = require('manta');
var MemoryStream = require('stream').PassThrough;
var path = require('path');

var obj = require('../../lib/obj.js');


///--- Globals

var MIN_UPLOAD_SIZE = 0;
var MAX_TEST_UPLOAD_SIZE = 1000;

var MIN_NUM_COPIES = 1;
// TODO: not sure if there's a way to get this value from the config.
var MAX_NUM_COPIES = 6;

var MIN_PART_NUM = 0;
var MAX_PART_NUM = 9999;

var TEXT = 'The lazy brown fox \nsomething \nsomething foo';


///--- Helpers

function ifErr(t, err, desc) {
    t.ifError(err, desc);
    if (err) {
        t.deepEqual(err.body, {}, desc + ': error body');
        return (true);
    }

    return (false);
}


function between(min, max) {
    return (Math.floor(Math.random() * (max - min + 1) + min));
}


function randomPartNum() {
    return (between(MIN_PART_NUM, MAX_PART_NUM));
}


function randomUploadSize() {
    return (between(MIN_UPLOAD_SIZE, MAX_TEST_UPLOAD_SIZE));
}


function randomNumCopies() {
    return (between(MIN_NUM_COPIES, MAX_NUM_COPIES));
}


// Given an account, id and (optionally) part number, returns a path that should
// be redirected
// (e.g. /jhendricks/uploads/d/d32e43e8-358f-42c0-aa8b-647a3d0f32f7)
function uploadPath(account, id, partNum) {
    var url = '/' + account + '/uploads/' + id.charAt(0) + '/' + id;

    if (!isNaN(partNum)) {
        url += '/' + partNum;
    }

    return (url);
}


// Given an account, id and (optionally) part number, returns a path that should
// be redirected (e.g. /jhendricks/uploads/d32e43e8-358f-42c0-aa8b-647a3d0f32f7)
function redirectPath(a, id, pn) {
    var p = '/' + a + '/uploads/' + id;
    if (!(pn === null || pn === undefined)) {
        p += '/' + pn;
    }

    return (p);
}


// Creates the options needed to upload string with account.
function createPartOptions(account, string) {
    var opts = {
        account: account,
        md5: crypto.createHash('md5').update(string).digest('base64'),
        size: Buffer.byteLength(string),
        type: 'text/plain'
    };

    return (opts);
}


function writeObject(client, id, partNum, opts, cb) {
    var stream = new MemoryStream();
    client.put(uploadPath(opts.account, id, partNum), stream, opts, cb);
    process.nextTick(stream.end.bind(stream, TEXT));
}


function computePartsMD5(parts) {
    var hash = crypto.createHash('md5');
    parts.forEach(function (p) {
        hash.update(p);
    });

    return (hash.digest('base64'));
}


function checkCreateResponse(t, o) {
    t.ok(o);

    // verify everything we expect to be in the response from create is there
    if (o) {
        t.ok(o.id);
        t.ok(o.partsDirectory);
        t.equal(o.id, path.basename(o.partsDirectory));
    }
}


function sanityCheckUpload(t, o, u) {
    t.ok(o);
    t.ok(u);

    // verify everything we expect to be in the response from get-mpu is there
    if (u) {
        t.ok(u.id);
        t.ok(u.uploadPath);
        t.ok(u.objectPath);
        t.ok(u.state);
        t.ok(u.headers);

        t.ok(u.state === 'created' || u.state === 'finalizing');
        if (u.state === 'finalizing') {
            t.ok(u.type);
            t.ok(u.type === 'commit' || u.type === 'abort');

            if (u.type === 'commit') {
                t.ok(u.partsMD5);
            }
        }
    }

    // verify that the response from create matches with what get-mpu said
    if (o && u) {
        t.equal(o.id, u.id);
        t.equal(o.partsDirectory, u.uploadPath);
    }
}

function createUploadHelper(s, account, subuser, p, h, cb) {
    var opts = manta.createOptions({
        contentType: 'application/json',
        accept: 'application/json',
        path: '/' + account + '/uploads'
    }, {});

    var client = s.client;
    if (subuser) {
        client = s.userClient;
    }

    client.signRequest({
        headers: opts.headers
    }, function (err) {
        if (err) {
            cb(err);
        } else {
            var body = {};
            if (p) {
                body.objectPath = p;
            }
            if (h) {
                body.headers = h;
            }

            s.client.jsonClient.post(opts, body,
            function (err2, req, res, o) {
                if (err2) {
                    cb(err2);
                } else {
                    cb(null, o);
                }
            });
        }
    });
}


function createUploadSubuser(s, account, p, h, cb) {
    createUploadHelper(s, account, true, p, h, cb);
}


function createUpload(s, account, p, h, cb) {
    createUploadHelper(s, account, false, p, h, cb);
}


///--- Exports

module.exports = {
    MIN_UPLOAD_SIZE: MIN_UPLOAD_SIZE,
    MAX_TEST_UPLOAD_SIZE: MAX_TEST_UPLOAD_SIZE,
    MIN_NUM_COPIES: MIN_NUM_COPIES,
    MAX_NUM_COPIES: MAX_NUM_COPIES,
    MIN_PART_NUM: MIN_PART_NUM,
    MAX_PART_NUM: MAX_PART_NUM,
    TEXT: TEXT,

    ifErr: ifErr,
    between: between,
    randomPartNum: randomPartNum,
    randomUploadSize: randomUploadSize,
    randomNumCopies: randomNumCopies,
    uploadPath: uploadPath,
    redirectPath: redirectPath,
    createPartOptions: createPartOptions,
    createUpload: createUpload,
    createUploadSubuser: createUploadSubuser,
    computePartsMD5: computePartsMD5,
    sanityCheckUpload: sanityCheckUpload,
    checkCreateResponse: checkCreateResponse,
    writeObject: writeObject
};
