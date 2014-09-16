/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var util = require('util');

var MemoryStream = require('stream').PassThrough;
var once = require('once');
var restify = require('restify');
var vasync = require('vasync');
var uuid = require('node-uuid');
var crypto = require('crypto');
var fs = require('fs');

var auth = require('../lib/auth');

var _helper = __dirname + '/helper.js';
if (require.cache[_helper])
    delete require.cache[_helper];
var helper = require(_helper);



///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;

var sprintf = util.format;

var SIG_FMT = 'Signature keyId="/%s/keys/%s",algorithm="%s",signature="%s"';
var TOKEN_CFG = {
    salt: process.env.MUSKIE_SALT,
    key: process.env.MUSKIE_KEY,
    iv: process.env.MUSKIE_IV,
    maxAge: process.env.MUSKIE_MAX_AGE || 604800000
};



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
    opts.signature = opts.signature || uuid.v4();

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

    var privatekey = opts.privatekey ||
        fs.readFileSync(process.env.HOME + '/.ssh/id_rsa', 'utf8');

    if (!ssoUrl || !ssoLogin || !ssoPassword || !privatekey) {
        cb();
        return;
    }

    var SSOclient = restify.createJsonClient({
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

before(function (cb) {
    var self = this;

    this.client = helper.createClient();
    this.rawClient = helper.createRawClient();
    this.root = '/' + this.client.user + '/stor';
    this.dir = this.root + '/' + uuid.v4();
    this.key = this.dir + '/' + uuid.v4();
    this.link = '/' + this.client.user + '/public/' + uuid.v4();

    var acct = {
        roles: {},
        account: {
            uuid: '930896af-bf8c-48d4-885c-6573a94b1853',
            login: 'poseidon',
            isOperator: true,
            groups: ['operators']
        }
    };

    var opts = {
        caller: acct
    };

    ['salt', 'key', 'iv'].forEach(function (env) {
        if (!TOKEN_CFG[env]) {
            cb(new Error('MUSKIE_' + env.toUpperCase() + ' required'));
            return;
        }
    });

    auth.createAuthToken(opts, TOKEN_CFG, function (err, token) {
        if (err) {
            cb(err);
            return;
        } else if (!token) {
            cb(new Error('no token'));
            return;
        }
        self.token = token;

        self.client.mkdir(self.dir, function (err2) {
            if (err2) {
                cb(err2);
                return;
            }

            writeObject(self.client, self.key, function (err3) {
                if (err3) {
                    cb(err3);
                    return;
                }

                self.client.ln(self.key, self.link, cb);
            });
        });
    });
});


after(function (cb) {
    var self = this;
    this.client.rmr(this.dir, function (err) {
        self.client.unlink(self.link, function (err2) {
            cb(err || err2);
        });
    });
});


test('access $self', function (t) {
    this.client.get(this.key, function (err, stream) {
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


test('auth with token (operator)', function (t) {
    var client = helper.createRawClient();
    var self = this;
    var opts = {
        path: '/poseidon/stor',
        headers: {
            authorization: 'Token ' + self.token
        }
    };
    client.get(opts, function (connect_err, req) {
        t.ifError(connect_err);
        if (connect_err) {
            t.end();
            return;
        }

        req.on('result', function (err, res) {
            t.ifError(err);
            res.once('end', function () {
                t.end();
            });
            res.resume();
        });
    });
});


test('auth caller not found', function (t) {
    var opts = {
        path: '/poseidon/stor',
        headers: {
            authorization:  authorization({
                user: uuid.v4()
            })
        }
    };
    rawRequest(opts, function (err, _, __, obj) {
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
            authorization:  authorization({
                keyId: uuid.v4()
            })
        }
    };
    rawRequest(opts, function (err, _, __, obj) {
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
            authorization:  authorization()
        }
    };
    rawRequest(opts, function (err, _, __, obj) {
        t.ok(err);
        t.equal(err.statusCode, 403);
        t.equal(err.restCode, 'InvalidSignature');
        t.ok(err.message);
        t.end();
    });
});


test('presigned URL no expires', function (t) {
    helper.signUrl(this.key, function (err, path) {
        t.ifError(err);
        t.ok(path);

        /* JSSTYLED */
        path = path.replace(/&expires=\d+/, '');
        rawRequest(path, function (err2, req, res, obj) {
            t.ok(err2);
            t.equal(res.statusCode, 403);
            t.equal(obj.code, 'InvalidQueryStringAuthentication');
            t.ok(obj.message);
            t.end();
        });
    });
});


test('presigned URL no keyid', function (t) {
    helper.signUrl(this.key, function (err, path) {
        t.ifError(err);
        t.ok(path);

        /* JSSTYLED */
        path = path.replace(/&keyId=.+&/, '&');
        rawRequest(path, function (err2, req, res, obj) {
            t.ok(err2);
            t.equal(res.statusCode, 403);
            t.equal(obj.code, 'InvalidQueryStringAuthentication');
            t.ok(obj.message);
            t.end();
        });
    });
});


test('presigned URL no algorithm', function (t) {
    helper.signUrl(this.key, function (err, path) {
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


test('presigned URL no signature', function (t) {
    helper.signUrl(this.key, function (err, path) {
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


test('presigned URL invalid signature', function (t) {
    var self = this;
    helper.signUrl(this.key, function (err, path) {
        t.ifError(err);
        t.ok(path);

        path = path.replace(self.key, self.key + '/' + uuid.v4());
        rawRequest(path, function (err2, req, res, obj) {
            t.ok(err2);
            t.equal(res.statusCode, 403);
            t.equal(obj.code, 'InvalidSignature');
            t.ok(obj.message);
            t.end();
        });
    });
});


test('presigned URL expired request', function (t) {
    var expiry = Math.floor((Date.now()/1000 - 10));
    helper.signUrl(this.key, expiry, function (err, path) {
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


test('presigned URL ok', function (t) {
    helper.signUrl(this.key, function (err, path) {
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


test('presigned URL ok, directory no trailing slash', function (t) {
    helper.signUrl(this.dir, function (err, path) {
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


test('presigned URL ok, directory trailing slash', function (t) {
    helper.signUrl(this.dir + '/', function (err, path) {
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


test('create auth token ok', function (t) {
    var opts = {
        method: 'POST',
        path: '/' + process.env.MANTA_USER + '/tokens'
    };
    helper.signUrl(opts, function (err, path) {
        t.ifError(err);
        t.ok(path);

        rawRequest({
            method: 'post',
            path: path
        }, function (err2, req, res, obj) {
            t.ifError(err2);
            t.equal(res.statusCode, 201);
            t.ok(obj);
            t.ok(obj.token);
            t.end();
        });
    });
});


test('anonymous get', function (t) {
    this.rawClient.get(this.link, function (err, req) {
        t.ifError(err);
        if (err) {
            t.end();
            return;
        }

        req.once('result', function (err2, res) {
            t.ifError(err2);

            var body = '';
            res.setEncoding('utf8');
            res.on('data', function (chunk) {
                body += chunk;
            });

            res.on('end', function () {
                t.ok(body);
                t.end();
            });
        });
    });
});


test('anonymous get 403', function (t) {
    this.rawClient.get(this.key, function (err, req) {
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


test('access 403 (fails if MANTA_USER is operator)', function (t) {
    writeObject(this.client, '/poseidon/public/agent.sh', function (err) {
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


test('access unapproved and operator /public', function (t) { // MANTA-2214
    this.client.get('/poseidon/public', function (err, stream) {
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


test('create auth token 403 (fails if MANTA_USER is operator)',
        function (t) {

    var opts = {
        method: 'POST',
        path: '/poseidon/tokens'
    };
    helper.signUrl(opts, function (err, path) {
        t.ifError(err);
        t.ok(path);

        rawRequest({
            method: 'post',
            path: path
        }, function (err2, req, res, obj) {
            t.ok(err2);
            t.equal(res.statusCode, 403);
            t.end();
        });
    });
});


if (process.env.SSO_URL) {
    var _AUTH_NAME = 'HTTP delegated auth token (fails if SSO_URL, SSO_LOGIN' +
        ', SSO_PASSWORD aren\'t set, or if MANTA_USER is not a registered ' +
        'developer)';
    test(_AUTH_NAME, function (t) {
        var opts = {
            ssoUrl: process.env.SSO_URL,
            ssoLogin: process.env.SSO_LOGIN,
            ssoPassword: process.env.SSO_PASSWORD,
            keyid: process.env.MANTA_KEY_ID,
            privatekey: fs.readFileSync(process.env.HOME + '/.ssh/id_rsa',
                                        'utf8')
        };
        var self = this;

        getHttpAuthToken(opts, function (token) {
            if (!token) {
                t.ifError('Could not retrieve token');
                t.end();
                return;
            }

            var url = '/' + process.env.SSO_LOGIN + '/stor';
            var _opts = {
                headers: {
                    'x-auth-token': token
                }
            };

            self.client.get(url, _opts, function (err, req, res, obj) {
                t.ifError(err);
                t.ok(res);
                t.end();
            });
        });
    });
}


test('HTTP delegated auth token with corrupt token', function (t) {
    var opts = {
      headers: {
        'x-auth-token': {data: 'invalid'}
      }
    };
    var self = this;
    var url = '/' + process.env.SSO_LOGIN + '/stor';

    self.client.get(url, opts, function (err, req, res, obj) {
        t.ok(err);
        t.end();
    });
});
