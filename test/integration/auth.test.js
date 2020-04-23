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
var test = require('@smaller/tap').test;
var uuidv4 = require('uuid/v4');

var auth = require('../../lib/auth');
var helper = require('../helper');




///--- Globals

var sprintf = util.format;

var SIG_FMT = 'Signature keyId="/%s/keys/%s",algorithm="%s",signature="%s"';


///--- Helpers

function writeObject(client, key, cb) {
    cb = once(cb);
    var input = new MemoryStream();
    var msg = JSON.stringify({hello: 'world'});
    var opts = {
        type: 'application/json',
        size: Buffer.byteLength(msg)
    };
    var output = client.createWriteStream(key, opts);
    output.once('close', cb.bind(null, null));
    output.once('error', cb);
    input.pipe(output);
    input.end(msg);
}


// Used to stub out signed requests
function authorization(opts) {
    opts = opts || {};
    opts.user = opts.user || process.env.MANTA_USER;
    opts.keyId = opts.keyId || process.env.MANTA_KEY_ID;
    opts.algorithm = opts.algorithm || 'rsa-sha256';
    opts.signature = opts.signature || uuidv4();

    var value = sprintf(SIG_FMT,
                        opts.user,
                        opts.keyId,
                        opts.algorithm,
                        opts.signature);

    return (value);
}


function rawRequest(opts, cb) {
    var client = helper.createJsonClient();
    if (opts.method === 'post') {
        client.post(opts, null, cb);
    } else {
        client.get(opts, cb);
    }
}


function getHttpAuthToken(opts, cb) {
    var ssoUrl = opts.ssoUrl || process.env.SSO_URL;
    var ssoLogin = opts.ssoLogin || process.env.SSO_LOGIN;
    var ssoPassword = opts.ssoPassword || process.env.SSO_PASSWORD;
    var keyid = opts.keyId || process.env.MANTA_KEY_ID;

    var privatekey = opts.privatekey || helper.getRegularPrivkey();

    if (!ssoUrl || !ssoLogin || !ssoPassword || !privatekey) {
        cb();
        return;
    }

    var SSOclient = restifyClients.createJsonClient({
        url: ssoUrl,
        rejectUnauthorized: false
    });

    var loginopts = {
        permissions: '{}',
        nonce: Math.random().toString(36).substring(7),
        keyid: '/' + process.env.MANTA_USER + '/keys/' + keyid,
        now: new Date().toUTCString()
    };

    var optkeys = Object.keys(loginopts).sort();
    var signstring = ssoUrl + '/login?';

    for (var i = 0; i < optkeys.length; i++) {
        signstring += optkeys[i] + '=' +
            encodeURIComponent(loginopts[optkeys[i]]) + '&';
    }
    signstring = signstring.slice(0, -1);

    var signer = crypto.createSign('sha256');
    signer.update(encodeURIComponent(signstring));
    var signature = signer.sign(privatekey, 'base64');

    loginopts['sig'] = signature;
    loginopts['username'] = ssoLogin;
    loginopts['password'] = ssoPassword;
    loginopts['permissions'] = '{}';
    SSOclient.post('/login', loginopts, function (err, req, res, obj) {
        if (err) {
            cb();
        } else {
            cb(JSON.stringify(obj.token));
        }
    });
}



///--- Tests


test('auth', function (suite) {
    var client;
    var rawClient;
    var dir;
    var key;
    var testUser;

    suite.test('setup: test user', function (t) {
        helper.ensureTestUser(function (err, user) {
            console.log('XXX testUser', user);
            t.ifError(err, 'no error ensuring test user');
            testUser = user;
            t.end();
        });
    });

    suite.test('setup', function (t) {
        client = helper.createClient();
        rawClient = helper.createRawClient();
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


    suite.test('access test object', function (t) {
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


    //// XXX token auth
    //suite.test('auth with bad token crypto', function (t) {
    //    var token_cfg = {
    //        salt: 'AAAAAAAAAAAAAAAA',
    //        key: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
    //        iv: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
    //        maxAge: 604800000
    //    };
    //
    //    var tokenopts = {
    //        caller: {
    //            account: {
    //                uuid: helper.POSEIDON_ID
    //            }
    //        }
    //    };
    //
    //    auth.createAuthToken(tokenopts, token_cfg, function (auth_err, token) {
    //        if (auth_err || !token) {
    //            t.ifError(auth_err || new Error('no token'));
    //            t.end();
    //            return;
    //        }
    //        var opts = {
    //            path: key,
    //            headers: {
    //                authorization: 'Token ' + token
    //            }
    //        };
    //        // XXX
    //        rawRequest(opts, function (err, res) {
    //            t.ok(err);
    //            if (err) {
    //                t.equal(err.statusCode, 403);
    //                t.equal(err.restCode, 'InvalidAuthenticationToken');
    //            }
    //            t.end();
    //        });
    //    });
    //});


    //// XXX token auth
    //suite.test('auth with token (operator)', function (t) {
    //    var tokenopts = {
    //        caller: {
    //            account: {
    //                uuid: helper.POSEIDON_ID
    //            }
    //        }
    //    };
    //
    //    helper.createAuthToken(tokenopts, function (err, token) {
    //        t.ifError(err);
    //        if (err) {
    //            t.end();
    //            return;
    //        }
    //        var client = helper.createRawClient();
    //        var opts = {
    //            path: '/poseidon/stor',
    //            headers: {
    //                authorization: 'Token ' + token
    //            }
    //        };
    //        client.get(opts, function (connect_err, req) {
    //            t.ifError(connect_err);
    //            if (connect_err) {
    //                t.end();
    //                return;
    //            }
    //
    //            req.on('result', function (result_err, res) {
    //                t.ifError(result_err);
    //                if (result_err) {
    //                    t.end();
    //                    return;
    //                }
    //                res.once('end', function () {
    //                    t.end();
    //                });
    //                res.resume();
    //            });
    //        });
    //    });
    //});


    suite.test('auth caller not found', function (t) {
        var opts = {
            path: '/poseidon/stor',
            headers: {
                authorization: authorization({
                    user: uuidv4()
                })
            }
        };
        rawRequest(opts, function (err) {
            t.ok(err);
            t.equal(err.statusCode, 403);
            t.equal(err.restCode, 'AccountDoesNotExist');
            t.ok(err.message);
            t.end();
        });
    });


    suite.test('auth key not found', function (t) {
        var opts = {
            path: '/poseidon/stor',
            headers: {
                authorization: authorization({
                    keyId: uuidv4()
                })
            }
        };
        rawRequest(opts, function (err) {
            t.ok(err);
            t.equal(err.statusCode, 403);
            t.equal(err.restCode, 'KeyDoesNotExist');
            t.ok(err.message);
            t.end();
        });
    });


    suite.test('signature invalid', function (t) {
        var opts = {
            path: '/poseidon/stor',
            headers: {
                authorization: authorization()
            }
        };
        rawRequest(opts, function (err) {
            t.ok(err);
            t.equal(err.statusCode, 403);
            t.equal(err.restCode, 'InvalidSignature');
            t.ok(err.message);
            t.end();
        });
    });


    suite.test('presigned URL no expires', function (t) {
        helper.signUrl(key, function (signErr, path) {
            t.ifError(signErr);
            t.ok(path);

            /* JSSTYLED */
            path = path.replace(/&expires=\d+/, '');
            rawRequest(path, function (err, _req, res, obj) {
                t.ok(err);
                t.equal(res.statusCode, 403);
                t.equal(obj.code, 'InvalidQueryStringAuthentication');
                t.ok(obj.message);
                t.end();
            });
        });
    });


    suite.test('presigned URL no keyid', function (t) {
        helper.signUrl(key, function (err, path) {
            t.ifError(err);
            t.ok(path);

            /* JSSTYLED */
            path = path.replace(/&keyId=.+&/, '&');
            rawRequest(path, function (err2, _req, res, obj) {
                t.ok(err2);
                t.equal(res.statusCode, 403);
                t.equal(obj.code, 'InvalidQueryStringAuthentication');
                t.ok(obj.message);
                t.end();
            });
        });
    });


    suite.test('presigned URL no algorithm', function (t) {
        helper.signUrl(key, function (err, path) {
            t.ifError(err);
            t.ok(path);

            /* JSSTYLED */
            path = path.replace(/algorithm=.+&/, '&');
            rawRequest(path, function (err2, req, res, obj) {
                t.ok(err2);
                t.equal(res.statusCode, 403);
                t.equal(obj.code, 'InvalidQueryStringAuthentication');
                t.ok(obj.message);
                t.end();
            });
        });
    });


    suite.test('presigned URL no signature', function (t) {
        helper.signUrl(key, function (err, path) {
            t.ifError(err);
            t.ok(path);

            /* JSSTYLED */
            path = path.replace(/&signature=.+/, '');
            rawRequest(path, function (err2, req, res, obj) {
                t.ok(err2);
                t.equal(res.statusCode, 403);
                t.equal(obj.code, 'InvalidQueryStringAuthentication');
                t.ok(obj.message);
                t.end();
            });
        });
    });


    suite.test('presigned URL invalid signature', function (t) {
        helper.signUrl(key, function (err, path) {
            t.ifError(err);
            t.ok(path);

            path = path.replace(key, key + '/' + uuidv4());
            rawRequest(path, function (err2, req, res, obj) {
                t.ok(err2);
                t.equal(res.statusCode, 403);
                t.equal(obj.code, 'InvalidSignature');
                t.ok(obj.message);
                t.end();
            });
        });
    });


    suite.test('presigned URL expired request', function (t) {
        var expiry = Math.floor((Date.now()/1000 - 10));
        helper.signUrl(key, expiry, function (err, path) {
            t.ifError(err);
            t.ok(path);

            rawRequest(path, function (err2, req, res, obj) {
                t.ok(err2);
                t.equal(res.statusCode, 403);
                t.equal(obj.code, 'InvalidQueryStringAuthentication');
                t.ok(obj.message);
                t.end();
            });
        });
    });


    suite.test('presigned URL ok', function (t) {
        helper.signUrl(key, function (err, path) {
            t.ifError(err);
            t.ok(path);

            rawRequest(path, function (err2, req, res, obj) {
                t.ifError(err2);
                t.equal(res.statusCode, 200);
                t.ok(obj);
                t.end();
            });
        });
    });


    suite.test('presigned URL ok, directory no trailing slash', function (t) {
        helper.signUrl(dir, function (err, path) {
            t.ifError(err);
            t.ok(path);

            rawRequest(path, function (err2, req, res, obj) {
                t.ifError(err2);
                t.equal(res.statusCode, 200);
                t.ok(obj);
                t.end();
            });
        });
    });


    suite.test('presigned URL ok, directory trailing slash', function (t) {
        helper.signUrl(dir + '/', function (err, path) {
            t.ifError(err);
            t.ok(path);

            rawRequest(path, function (err2, req, res, obj) {
                t.ifError(err2);
                t.equal(res.statusCode, 200);
                t.ok(obj);
                t.end();
            });
        });
    });


    // XXX
    //suite.test('create auth token ok', function (t) {
    //    var opts = {
    //        method: 'POST',
    //        path: '/' + process.env.MANTA_USER + '/tokens'
    //    };
    //    helper.signUrl(opts, function (err, path) {
    //        t.ifError(err);
    //        t.ok(path);
    //
    //        rawRequest({
    //            method: 'post',
    //            path: path
    //        }, function (err2, req, res, obj) {
    //            t.ifError(err2);
    //            t.equal(res.statusCode, 201);
    //            t.ok(obj);
    //            t.ok(obj.token);
    //            t.end();
    //        });
    //    });
    //});



    suite.test('anonymous get 403', function (t) {
        rawClient.get(key, function (err, req) {
            t.ifError(err);
            if (err) {
                t.end();
                return;
            }

            req.once('result', function (err2, res) {
                t.ok(err2);
                t.end();
            });
        });
    });


    suite.test('access 403 (fails if MANTA_USER is operator)', function (t) {
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


    suite.test('access unapproved and operator /public', function (t) { // MANTA-2214
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


    // XXX
    //suite.test('create auth token 403 (fails if MANTA_USER is operator)',
    //        function (t) {
    //
    //    var opts = {
    //        method: 'POST',
    //        path: '/poseidon/tokens'
    //    };
    //    helper.signUrl(opts, function (err, path) {
    //        t.ifError(err);
    //        t.ok(path);
    //
    //        rawRequest({
    //            method: 'post',
    //            path: path
    //        }, function (err2, req, res, obj) {
    //            t.ok(err2);
    //            t.equal(res.statusCode, 403);
    //            t.end();
    //        });
    //    });
    //});
    //
    //
    //if (process.env.SSO_URL) {
    //    var _AUTH_NAME = 'HTTP delegated auth token (fails if SSO_URL, SSO_LOGIN' +
    //        ', SSO_PASSWORD aren\'t set, or if MANTA_USER is not a registered ' +
    //        'developer)';
    //    suite.test(_AUTH_NAME, function (t) {
    //        var opts = {
    //            ssoUrl: process.env.SSO_URL,
    //            ssoLogin: process.env.SSO_LOGIN,
    //            ssoPassword: process.env.SSO_PASSWORD,
    //            keyid: process.env.MANTA_KEY_ID,
    //            privatekey: helper.getRegularPrivkey()
    //        };
    //
    //        getHttpAuthToken(opts, function (token) {
    //            if (!token) {
    //                t.ifError('Could not retrieve token');
    //                t.end();
    //                return;
    //            }
    //
    //            var url = '/' + process.env.SSO_LOGIN + '/stor';
    //            var _opts = {
    //                headers: {
    //                    'x-auth-token': token
    //                }
    //            };
    //
    //            client.get(url, _opts, function (err, req, res, obj) {
    //                t.ifError(err);
    //                t.ok(res);
    //                t.end();
    //            });
    //        });
    //    });
    //
    //    suite.test('HTTP delegated auth token with missing signature', function (t) {
    //        var opts = {
    //            ssoUrl: process.env.SSO_URL,
    //            ssoLogin: process.env.SSO_LOGIN,
    //            ssoPassword: process.env.SSO_PASSWORD,
    //            keyid: process.env.MANTA_KEY_ID,
    //            privatekey: helper.getRegularPrivkey()
    //        };
    //
    //        getHttpAuthToken(opts, function (token) {
    //            if (!token) {
    //                t.ifError('Could not retrieve token');
    //                t.end();
    //                return;
    //            }
    //
    //            var url = '/' + process.env.SSO_LOGIN + '/stor';
    //            var _opts = {
    //                headers: {
    //                    'x-auth-token': token
    //                },
    //                path: url
    //            };
    //
    //            rawClient.get(_opts, function (err, req) {
    //                t.ifError(err);
    //                if (err) {
    //                    t.end();
    //                    return;
    //                }
    //
    //                req.once('result', function (err2, res) {
    //                    t.ok(err2);
    //                    if (!err2) {
    //                        t.end();
    //                        return;
    //                    }
    //                    t.equal(err2.statusCode === 401);
    //                    t.end();
    //                });
    //            });
    //        });
    //    });
    //}
    //
    //
    //suite.test('HTTP delegated auth token with corrupt token', function (t) {
    //    var opts = {
    //      headers: {
    //        'x-auth-token': {data: 'invalid'}
    //      }
    //    };
    //    var url = '/' + process.env.SSO_LOGIN + '/stor';
    //
    //    client.get(url, opts, function (err, req, res, obj) {
    //        t.ok(err);
    //        t.end();
    //    });
    //});


    suite.test('teardown', function (t) {
        client.rmr(dir, function onRm(err) {
            t.ifError(err, 'remove test dir ' + dir);
            t.end();
        });
    });

    suite.end();
});
