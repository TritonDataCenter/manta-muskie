/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var crypto = require('crypto');
var util = require('util');

var MemoryStream = require('stream').PassThrough;
var once = require('once');
var restifyClients = require('restify-clients');
var test = require('tap').test;
var uuidv4 = require('uuid/v4');

var auth = require('../../lib/auth');
var helper = require('../helper');


///--- Helpers

function writeObject(client_, key_, cb) {
    cb = once(cb);
    var input = new MemoryStream();
    var msg = JSON.stringify({hello: 'world'});
    var opts = {
        type: 'application/json',
        size: Buffer.byteLength(msg)
    };
    var output = client_.createWriteStream(key_, opts);
    output.once('close', cb.bind(null, null));
    output.once('error', cb);
    input.pipe(output);
    input.end(msg);
}


function bogusSigAuthHeader(user, keyId) {
    // "Bogus" because we don't generate a valid "signature" value.
    // JSSTYLED
    return util.format('Signature keyId="/%s/keys/%s",algorithm="rsa-sha256",signature="%s"',
        user, keyId, uuidv4());
}


///--- Tests

var client;
var dir;
var jsonClient = helper.createJsonClient();
var key;
var stringClient = helper.createStringClient();
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

test('setup: test dir and object', function (t) {
    client = helper.mantaClientFromAccountInfo(testAccount);
    var root = '/' + client.user + '/stor';
    dir = root + '/test-auth-dir-' + uuidv4().split('-')[0];
    key = dir + '/test-auth-file-' + uuidv4().split('-')[0];

    client.mkdir(dir, function (err) {
        t.ifError(err, 'no error making test dir ' + dir);
        if (err) {
            t.end();
            return;
        }

        writeObject(client, key, function (writeErr) {
            t.ifError(writeErr, 'no error writing test key ' + key);
            t.end();
        });
    });
});

test('access test object', function (t) {
    client.get(key, function (err, stream) {
        t.ifError(err, 'no error from GetObject');
        t.ok(stream, 'have a GetObject stream');
        if (stream) {
            stream.once('end', function onEnd() {
                t.end();
            });
            stream.resume();
        } else {
            t.end();
        }
    });
});

test('auth caller not found', function (t) {
    var opts = {
        path: '/poseidon/stor',
        headers: {
            // Bogus `login`.
            authorization: bogusSigAuthHeader(uuidv4(), testAccount.fp)
        }
    };
    jsonClient.get(opts, function (err) {
        t.ok(err);
        t.equal(err.statusCode, 403);
        t.equal(err.restCode, 'AccountDoesNotExist');
        t.ok(err.message);
        t.end();
    });
});


test('auth key not found', function (t) {
    var opts = {
        path: '/poseidon/stor',
        headers: {
            // Bogus `keyId`.
            authorization: bogusSigAuthHeader(testAccount.login, uuidv4())
        }
    };
    jsonClient.get(opts, function (err) {
        t.ok(err);
        t.equal(err.statusCode, 403);
        t.equal(err.restCode, 'KeyDoesNotExist');
        t.ok(err.message);
        t.end();
    });
});


test('signature invalid', function (t) {
    var opts = {
        path: '/poseidon/stor',
        headers: {
            // Bogus "signature".
            authorization: bogusSigAuthHeader(
                testAccount.login, testAccount.fp)
        }
    };
    jsonClient.get(opts, function (err) {
        t.ok(err);
        t.equal(err.statusCode, 403);
        t.equal(err.restCode, 'InvalidSignature');
        t.ok(err.message);
        t.end();
    });
});


test('signed URL no expires', function (t) {
    client.signURL({
        path: key
    }, function (signErr, path) {
        t.ifError(signErr);
        t.ok(path);

        /* JSSTYLED */
        path = path.replace(/&expires=\d+/, '');
        jsonClient.get(path, function (err, _req, res, obj) {
            t.ok(err);
            t.equal(res.statusCode, 403);
            t.equal(obj.code, 'InvalidQueryStringAuthentication');
            t.ok(obj.message);
            t.end();
        });
    });
});


test('signed URL no keyid', function (t) {
    client.signURL({
        path: key
    }, function (signErr, path) {
        t.ifError(signErr);
        t.ok(path);

        /* JSSTYLED */
        path = path.replace(/&keyId=.+&/, '&');
        jsonClient.get(path, function (err, _req, res, obj) {
            t.ok(err);
            t.equal(res.statusCode, 403);
            t.equal(obj.code, 'InvalidQueryStringAuthentication');
            t.ok(obj.message);
            t.end();
        });
    });
});


test('signed URL no algorithm', function (t) {
    client.signURL({
        path: key
    }, function (signErr, path) {
        t.ifError(signErr);
        t.ok(path);

        /* JSSTYLED */
        path = path.replace(/algorithm=.+&/, '&');
        jsonClient.get(path, function (err, _req, res, obj) {
            t.ok(err);
            t.equal(res.statusCode, 403);
            t.equal(obj.code, 'InvalidQueryStringAuthentication');
            t.ok(obj.message);
            t.end();
        });
    });
});


test('signed URL no signature', function (t) {
    client.signURL({
        path: key
    }, function (signErr, path) {
        t.ifError(signErr);
        t.ok(path);

        /* JSSTYLED */
        path = path.replace(/&signature=.+/, '');
        jsonClient.get(path, function (err, _req, res, obj) {
            t.ok(err);
            t.equal(res.statusCode, 403);
            t.equal(obj.code, 'InvalidQueryStringAuthentication');
            t.ok(obj.message);
            t.end();
        });
    });
});


test('signed URL invalid signature', function (t) {
    client.signURL({
        path: key
    }, function (signErr, path) {
        t.ifError(signErr);
        t.ok(path);

        path = path.replace(key, key + '/' + uuidv4());
        jsonClient.get(path, function (err, _req, res, obj) {
            t.ok(err);
            t.equal(res.statusCode, 403);
            t.equal(obj.code, 'InvalidSignature');
            t.ok(obj.message);
            t.end();
        });
    });
});


test('signed URL expired request', function (t) {
    client.signURL({
        path: key,
        expires: Math.floor(Date.now() / 1000 - 10)
    }, function (signErr, path) {
        t.ifError(signErr);
        t.ok(path);

        jsonClient.get(path, function (err, _req, res, obj) {
            t.ok(err);
            t.equal(res.statusCode, 403);
            t.equal(obj.code, 'InvalidQueryStringAuthentication');
            t.ok(obj.message);
            t.end();
        });
    });
});


test('fail to signURL for another account', function (t) {
    client.signURL({
        path: '/poseidon/stor'
    }, function (signErr, path) {
        t.ifError(signErr);
        t.ok(path);

        jsonClient.get(path, function (getErr, _req, res, obj) {
            t.ok(getErr);
            t.equal(res.statusCode, 403, '403 response status');
            t.end();
        });
    });
});


test('signed URL ok', function (t) {
    client.signURL({
        path: key
    }, function (signErr, path) {
        t.ifError(signErr);
        t.ok(path);

        jsonClient.get(path, function (err, _req, res, obj) {
            t.ifError(err);
            t.equal(res.statusCode, 200);
            t.ok(obj);
            t.end();
        });
    });
});


test('signed URL ok, directory no trailing slash', function (t) {
    client.signURL({
        path: dir
    }, function (signErr, path) {
        t.ifError(signErr);
        t.ok(path);

        jsonClient.get(path, function (err, _req, res, obj) {
            t.ifError(err);
            t.equal(res.statusCode, 200);
            t.ok(obj);
            t.end();
        });
    });
});


test('signed URL ok, directory trailing slash', function (t) {
    client.signURL({
        path: dir + '/'
    }, function (signErr, path) {
        t.ifError(signErr);
        t.ok(path);

        jsonClient.get(path, function (err, _req, res, obj) {
            t.ifError(err);
            t.equal(res.statusCode, 200);
            t.ok(obj);
            t.end();
        });
    });
});


test('anonymous get 403', function (t) {
    stringClient.get(key, function (err, _req, res) {
        t.ok(err);
        t.equal(res.statusCode, 403, '403 response status');
        t.end();
    });
});


test('access 403', function (t) {
    writeObject(client, '/poseidon/public/agent.sh', function (err) {
        t.ok(err);
        if (!err) {
            t.end();
            return;
        }
        t.equal(err.name, 'AuthorizationFailedError');
        t.ok(err.message);
        t.end();
    });
});


// MANTA-2214
test('access unapproved and operator /public', function (t) {
    client.get('/poseidon/public', function (err, stream) {
        t.ifError(err);
        t.ok(stream);
        if (stream) {
            stream.once('end', t.end.bind(t));
            stream.resume();
        } else {
            t.end();
        }
    });
});


test('teardown', function (t) {
    client.rmr(dir, function onRm(err) {
        t.ifError(err, 'remove test dir ' + dir);
        t.end();
    });
});
