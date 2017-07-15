/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var crypto = require('crypto');
var jsprim = require('jsprim');
var fs = require('fs');
var manta = require('manta');
var MemoryStream = require('stream').PassThrough;
var obj = require('../../lib/obj');
var path = require('path');
var sshpk = require('sshpk');
var util = require('util');
var uuid = require('node-uuid');


if (require.cache[path.join(__dirname, '/../helper.js')])
    delete require.cache[path.join(__dirname, '/../helper.js')];
var testHelper = require('../helper.js');


///--- Globals

var sprintf = util.format;

var MIN_UPLOAD_SIZE = 0;
var MAX_TEST_UPLOAD_SIZE = 1000;

var MIN_NUM_COPIES = obj.DEF_MIN_COPIES;
var MAX_NUM_COPIES = obj.DEF_MAX_COPIES;

var MIN_PART_NUM = 0;
var MAX_PART_NUM = 9999;

var ZERO_BYTE_MD5 = obj.ZERO_BYTE_MD5;

var TEXT = 'The lazy brown fox \nsomething \nsomething foo';
var TEXT_MD5 = crypto.createHash('md5').update(TEXT).digest('base64');

/*
 * We need an operator account for some tests, so we use poseidon, unless an
 * alternate one is provided.
 */
var TEST_OPERATOR = process.env.MUSKIETEST_OPERATOR_USER || 'poseidon';
var TEST_OPERATOR_KEY = process.env.MUSKIETEST_OPERATOR_KEYFILE ||
        (process.env.HOME + '/.ssh/id_rsa_poseidon');


///--- Helpers

/*
 * All MPU-related tests should use this in the test setup code, and the
 * complimentary cleanupMPUTester function in the test teardown code.
 *
 * This function sets up some helper methods on the tester object that provide
 * are cleaned up properly.  Specifically, when an upload is created, the
 * `uploadId` field is set to the MPU's upload ID. If it is finalized using
 * the helper methods {abort,commit}Upload, the `uploadFinalized` flag is set.
 * On teardown, we use these flags to ensure all uploads are finalized so that
 * they will be garbage collected.
 */
function initMPUTester(tcb) {
    var self = this;

    self.client = testHelper.createClient();
    self.userClient = testHelper.createUserClient('muskie_test_user');
    self.operatorClient = createOperatorClient(TEST_OPERATOR,
        TEST_OPERATOR_KEY);

    self.uploadsRoot = '/' + self.client.user + '/uploads';
    self.root = '/' + self.client.user + '/stor';
    self.dir = self.root + '/' + uuid.v4();
    self.path = self.dir + '/' + uuid.v4();

    self.uploadId = null;
    self.partsDirectory = null;
    self.uploadFinalized = false;

    self.mskPrefixLength = null;

    // Thin wrappers around the MPU API.
    self.createUpload = function create(p, headers, cb) {
        createUploadHelper.call(self, p, headers, self.client, cb);
    };
    self.abortUpload = function abort(id, cb) {
        abortUploadHelper.call(self, id, self.client, cb);
    };
    self.commitUpload = function commit(id, etags, cb) {
        commitUploadHelper.call(self, id, etags, self.client, cb);
    };
    self.getUpload = function get(id, cb) {
        getUploadHelper.call(self, id, self.client, cb);
    };
    self.writeTestObject = function writeObject(id, partNum, cb) {
        writeObjectHelper.call(self, id, partNum, TEXT, self.client, cb);
    };

    // Wrappers using subusers as the caller.
    self.createUploadSubuser = function createSubuser(p, headers, cb) {
        createUploadHelper.call(self, p, headers, self.userClient, cb);
    };
    self.abortUploadSubuser = function abortSubuser(id, cb) {
        abortUploadHelper.call(self, id, self.userClient, cb);
    };
    self.commitUploadSubuser = function commitSubuser(id, etags, cb) {
        commitUploadHelper.call(self, id, etags, self.userClient, cb);
    };
    self.getUploadSubuser = function getSubuser(id, cb) {
        getUploadHelper.call(self, id, self.userClient, cb);
    };
    self.writeTestObjectSubuser = function writeObjectSubuser(id, partNum, cb) {
        writeObjectHelper.call(self, id, partNum, TEXT, self.userClient, cb);
    };

   /*
    * Returns the path a client can use to get redirected to the real
    * partsDirectory of a created MPU.
    *
    * Inputs:
    * - pn: optional part num to include in the path
    */
    self.redirectPath = function redirectPath(pn) {
        assert.ok(self.uploadId);
        var p = self.uploadsRoot + '/' + self.uploadId;
        if (typeof (pn) === 'number') {
            p += '/' + pn;
        }
        return (p);
    };

   /*
    * Returns the partsDirectory of a created MPU.
    *
    * Inputs:
    * - pn: optional part num to include in the path
    */
    self.uploadPath = function uploadPath(pn) {
        var p;
        if (self.partsDirectory) {
            p = self.partsDirectory;
        } else {
            assert.ok(self.uploadId, 'self.uploadId');
            var c = self.uploadId.charAt(self.uploadId.length - 1);
            var len = jsprim.parseInteger(c, { base: 16 });
            if ((typeof (len) !== 'number') || (len < 1) || (len > 4)) {
                len = 1;
            }

            var prefix = self.uploadId.substring(0, len);
            p = '/' + self.client.user + '/uploads/' + prefix + '/' +
                self.uploadId;
        }

        if (typeof (pn) === 'number') {
            p += '/' + pn;
        }
        return (p);
    };

    self.client.mkdir(self.dir, function (mkdir_err) {
        if (mkdir_err) {
            tcb(mkdir_err);
        } else {
            tcb(null);
        }
    });
}

/*
 * All MPU-related tests should use this in the test teardown code, and the
 * complimentary initMPUTester function in the test setup code.
 *
 * This function ensures that if an upload was created and not finalized by the
 * test, it is aborted, so that the MPU can be garbage collected. It also
 * closes open clients and removes test objects created as a part of the test.
 */
function cleanupMPUTester(cb) {
    var self = this;
    function closeClients(ccb) {
        this.client.close();
        this.userClient.close();
        ccb();
    }

    self.client.rmr(self.dir, function () {
        if (self.uploadId && !self.uploadFinalized) {
            var opts = {
                account: self.client.user,
                partsDirectory: self.uploadPath()
            };
            self.client.abortUpload(self.uploadId, opts,
                closeClients.bind(self, cb));
        } else {
            closeClients.call(self, cb);
        }
    });
}


/*
 * Helper to create a Manta client for the operator account.
 *
 * Parameters:
 *  - user: the operator account
 *  - keyFile: local path to the private key for this account
 */
function createOperatorClient(user, keyFile) {
    var key = fs.readFileSync(keyFile);
    var keyId = sshpk.parseKey(key, 'auto').fingerprint('md5').toString();

    var log = testHelper.createLogger();
    var client = manta.createClient({
        agent: false,
        connectTimeout: 2000,
        log: log,
        retry: false,
        sign: manta.privateKeySigner({
            key: key,
            keyId: keyId,
            log: log,
            user: user
        }),
        rejectUnauthorized: false,
        url: process.env.MANTA_URL || 'http://localhost:8080',
        user: user
    });

    return (client);
}


/*
 * Helper that creates an upload and passes the object returned from `create`
 * to the callback. On success, it will do a basic sanity check on the object
 * returned by create using the tester.
 *
 * Parameters:
 *  - p: the target object path to pass to `create`
 *  - h: a headers object to pass to `create`
 *  - client: client to use for the request
 *  - cb: callback of the form cb(err, object)
 */
function createUploadHelper(p, h, client, cb) {
    var self = this;
    assert.object(self);
    assert.object(client);
    assert.func(cb);

    var opts = {
        headers: {
            'content-type': 'application/json',
            'expect': 'application/json'
        },
        path: self.uploadsRoot
    };

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

            self.client.jsonClient.post(opts, body,
            function (err2, req, res, o) {
                if (err2) {
                    cb(err2);
                } else {
                    self.uploadId = o.id;
                    self.partsDirectory = o.partsDirectory;
                    var err3 = checkCreateResponse(o);
                    if (err3) {
                        cb(err2);
                    } else {
                        cb(null, o);
                    }
                }
            });
        }
    });
}

/*
 * Helper that gets a created upload and passes the object returned
 * to the callback. On success, it will do a basic sanity check on the object
 * returned.
 *
 * Parameters:
 *  - id: the upload ID to `get`
 *  - client: client to use for the request
 *  - cb: callback of the form cb(err, upload)
 */
function getUploadHelper(id, client, cb) {
    var self = this;
    assert.object(self);
    assert.string(id);
    assert.object(client);
    assert.func(cb);

    var opts = {
        account: self.client.user,
        partsDirectory: self.uploadPath()
    };

    client.getUpload(id, opts, function (err, upload) {
        if (err) {
            cb(err);
        } else {
            var err2 = checkGetResponse.call(self, upload);
            if (err2) {
                cb(err2);
            } else {
                cb(null, upload);
            }
        }
    });
}

/*
 * Helper that aborts an MPU and sets the `uploadFinalized` flag on the tester
 * object.
 *
 * Parameters:
 *  - id: the upload ID
 *  - client: client to use for the request
 *  - cb: callback of the form cb(err)
 */
function abortUploadHelper(id, client, cb) {
    var self = this;
    assert.object(self);
    assert.string(id);
    assert.object(client);
    assert.func(cb);

    var opts = {
        account: self.client.user,
        partsDirectory: self.uploadPath()
    };

    client.abortUpload(id, opts, function (err) {
        if (err) {
            cb(err);
        } else {
            self.uploadFinalized = true;
            cb();
        }
    });
}


/*
 * Helper that commits an MPU and sets the `uploadFinalized` flag on the tester
 * object.
 *
 * Parameters:
 *  - id: the upload ID
 *  - etags: an array of etags representing parts to commit
 *  - client: client to use for the request
 *  - cb: callback of the form cb(err)
 */
function commitUploadHelper(id, etags, client, cb) {
    var self = this;
    assert.object(self);
    assert.string(id);
    assert.object(client);
    assert.func(cb);

    var opts = {
        account: self.client.user,
        partsDirectory: self.uploadPath()
    };

    client.commitUpload(id, etags, opts, function (err, res) {
        if (err) {
            cb(err);
        } else {
            self.uploadFinalized = true;
            cb(null, res);
        }
    });
}


/*
 * Uploads a test object to an upload.
 *
 * Parameters:
 *  - id: the upload ID
 *  - partNum: the part number
 *  - string: string representing the object data
 *  - client: client to use for the request
 *  - cb: callback of the form cb(err, res)
 */
function writeObjectHelper(id, partNum, string, client, cb) {
    var self = this;
    assert.object(self);
    assert.string(string);
    assert.object(client);
    assert.func(cb);

    var opts = {
        account: self.client.user,
        md5: crypto.createHash('md5').update(string).digest('base64'),
        size: Buffer.byteLength(string),
        type: 'text/plain',
        partsDirectory: self.uploadPath()
    };

    var stream = new MemoryStream();
    client.put(self.uploadPath(partNum), stream, opts, cb);
    setImmediate(stream.end.bind(stream, string));
}


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
    return (between(MIN_NUM_COPIES, 3));
}


// Given an array of etags, returns the md5 we expect from the MPU API.
function computePartsMD5(parts) {
    var hash = crypto.createHash('md5');
    parts.forEach(function (p) {
        hash.update(p);
    });

    return (hash.digest('base64'));
}


/*
 * Verifies that the response sent by muskie on create-mpu is correct. If it's
 * not, we return an error.
 *
 * Inputs:
 *  - o: the object returned from create-mpu
 */
function checkCreateResponse(o) {
    if (!o) {
        return (new Error('create-mpu returned no response'));
    }

    if (!o.id) {
        return (new Error('create-mpu did not return an upload ID'));
    }

    if (!o.partsDirectory) {
        return (new Error('create-mpu did not return a parts directory'));
    }

    if (!(o.id === path.basename(o.partsDirectory))) {
        return (new Error('create-mpu returned an upload ID that does not ' +
            'match its parts directory'));
    }

    return (null);
}

/*
 * Verifies that the response sent by muskie on get-mpu is correct. If anything
 * is wrong, we return an error.
 *
 * Inputs:
 *  - u: the object returned from get-mpu
 */
function checkGetResponse(u) {
    if (!u) {
        return (new Error('get-mpu returned no response'));
    }

    if (!u.id) {
        return (new Error('get-mpu did not return an upload ID'));
    }

    // Verify that the id from create-mpu matches what get-mpu said.
    if (this.uploadId) {
        if (this.uploadId !== u.id) {
            return (new Error(sprintf('get-mpu returned an upload with a ' +
                'different id than was returned from create-mpu: ' +
                'expected id "%s", but got id "%s"',
                this.uploadId, u.id)));
        }

        if (u.state === 'created') {
            if (this.partsDirectory !== u.partsDirectory) {
                return (new Error(sprintf('get-mpu returned an upload with a ' +
                    'different partsDirectory than was returned from ' +
                    'create-mpu: expected partsDirectory "%s", but got "%s"',
                    this.partsDirectory, u.partsDirectory)));
            }
        }
    }

    if (!u.state) {
        return (new Error('get-mpu did not return an upload state'));
    }

    if (!(u.state === 'created' || u.state === 'finalizing' ||
       u.state === 'done')) {
        return (new Error(sprintf('get-mpu returned an invalid state: %s',
            u.state)));
    }

    if (!u.targetObject) {
        return (new Error('get-mpu did not return the target object path'));
    }

    if (!u.headers) {
        return (new Error('get-mpu did not return the target object headers'));
    }

    if (!u.numCopies) {
        return (new Error('get-mpu did not return numCopies for the target ' +
            'object'));
    }

    if (!u.headers) {
        return (new Error('get-mpu did not return the target object headers'));
    }

    if (!u.creationTimeMs) {
        return (new Error('get-mpu did not return the mpu creation time'));
    }

    if (u.state === 'created') {
        // A created upload will have 'partsDirectory', but finalized uploads
        // will not.
        if (!u.partsDirectory) {
            return (new Error('get-mpu returned an upload in state "created" ' +
                'with no "partsDirectory" field'));
        }
    } else if (u.state === 'finalizing') {
        if (!u.type) {
            return (new Error('get-mpu returned an upload in state ' +
                '"finalizing" with no "type" field'));
        }
        if (!(u.type === 'abort' || u.type === 'commit')) {
            return (new Error(sprintf('get-mpu returned an upload in state ' +
                '"finalizing" with invalid "type": %s'), u.type));
        }

    } else {
        // Uploads in state "done" have a "result" field that specifies whether
        // it was committed or aborted, but no type.
        if (!u.result) {
            return (new Error('get-mpu returned an upload in state ' +
                '"done" with no "result" field'));
        }
        if (!(u.result === 'aborted' || u.result === 'committed')) {
            return (new Error(sprintf('get-mpu returned an upload in state ' +
                '"finalizing" with invalid "result": %s', u.result)));
        }
        if (u.result === 'committed') {
            if (!u.partsMD5Summary) {
                return (new Error('get-mpu returned an upload in state ' +
                    '"finalizing", result "committed", with no ' +
                    '"partsMD5Summary" field'));
            }
        }
    }

    return (null);
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
    TEXT_MD5: TEXT_MD5,
    ZERO_BYTE_MD5: ZERO_BYTE_MD5,

    cleanupMPUTester: cleanupMPUTester,
    initMPUTester: initMPUTester,
    ifErr: ifErr,
    between: between,
    randomPartNum: randomPartNum,
    randomUploadSize: randomUploadSize,
    randomNumCopies: randomNumCopies,
    computePartsMD5: computePartsMD5
};
