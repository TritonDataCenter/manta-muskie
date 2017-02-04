/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

//
// Generate keys for the muskie config with:
//
// $ openssl enc -aes-128-cbc -k $(uuid) -P
// salt=C93A670ACC05C166
// key=5163205CA0C7F2752FD3A574E30F64DD
// iv=6B11F0F0B786F96812D5A0799D5B217A
//

var crypto = require('crypto');
var zlib = require('zlib');

var assert = require('assert-plus');
var httpSignature = require('http-signature');
var path = require('path');
var restify = require('restify');
var sprintf = require('util').format;
var vasync = require('vasync');
var libmanta = require('libmanta');
var libuuid = require('libuuid');
var xtend = require('xtend');

var common = require('./common');
require('./errors');



///--- Messages

var TOKEN_ALG = 'aes-128-cbc';
var SIGN_ALG = {
    'RSA-SHA1': true,
    'RSA-SHA256': true,
    'RSA-SHA384': true,
    'RSA-SHA512': true,
    'DSA-SHA1': true,
    'DSA-SHA256': true,
    'ECDSA-SHA256': true,
    'ECDSA-SHA384': true,
    'ECDSA-SHA512': true
};


///--- Helpers

function rfc3986(str) {
    /* JSSTYLED */
    return (encodeURIComponent(str)
            /* JSSTYLED */
            .replace(/[!'()]/g, escape)
            /* JSSTYLED */
            .replace(/\*/g, '%2A'));
}

function createAuthToken(opts, aes, cb) {
    assert.object(opts, 'opts');
    assert.object(opts.caller, 'opts.caller');
    assert.optionalObject(opts.context, 'opts.context');
    assert.optionalBool(opts.fromjob, 'opts.fromjob');
    assert.object(aes, 'aes');
    assert.string(aes.salt, 'aes.salt');
    assert.string(aes.key, 'aes.key');
    assert.string(aes.iv, 'aes.iv');
    assert.func(cb, 'callback');

    var cipher = crypto.createCipheriv(TOKEN_ALG,
                                       new Buffer(aes.key, 'hex'),
                                       new Buffer(aes.iv, 'hex'));
    assert.ok(cipher, 'failed to create crypto cipher');

    var caller = opts.caller;
    var context = opts.context;
    var fromjob = opts.fromjob;

    var principal = {
        account: null,
        user: null,
        roles: caller.roles || null
    };

    /*
     * Pick out the context conditions that should be contained in the token.
     * This should include any Manta-defined conditions that are used by Manta
     * itself during authorization, like activeRoles, but shouldn't include any
     * other conditions like date or sourceip, which aren't used by Manta.
     */
    var conditions = {};
    if (context && context.conditions) {
        conditions.activeRoles = context.conditions.activeRoles;
    }
    if (opts.fromjob) {
        conditions.fromjob = fromjob;
    }

    if (caller.account) {
        principal.account = {
            uuid: caller.account.uuid
        };
    }

    if (caller.user) {
        principal.user = {
            uuid: caller.user.uuid
        };
    }

    var str = JSON.stringify({
        t: Date.now(),
        p: principal,
        c: conditions,
        v: 2
    });

    zlib.gzip(new Buffer(str, 'utf8'), function (err, buf) {
        if (err) {
            cb(err);
            return;
        }

        var token = cipher.update(buf, 'binary', 'base64');
        token += cipher.final('base64');
        cb(null, token);
    });
}


function parseAuthToken(token, aes, cb) {
    assert.string(token, 'token');
    assert.object(aes, 'aes');
    assert.string(aes.salt, 'aes.salt');
    assert.string(aes.key, 'aes.key');
    assert.string(aes.iv, 'aes.iv');
    assert.number(aes.maxAge, 'aes.maxAge');
    assert.func(cb, 'callback');


    var decipher = crypto.createDecipheriv(TOKEN_ALG,
                                           new Buffer(aes.key, 'hex'),
                                           new Buffer(aes.iv, 'hex'));
    assert.ok(decipher, 'failed to create crypto cipher');
    var buf;
    try {
        buf = decipher.update(token, 'base64', 'binary');
        buf += decipher.final('binary');
    } catch (e) {
        setImmediate(function () {
            cb(new InvalidAuthTokenError());
        });
        return;
    }

    zlib.gunzip(new Buffer(buf, 'binary'), function (err, str) {
        if (err) {
            cb(new InvalidAuthTokenError());
            return;
        }

        var cracked;
        try {
            cracked = JSON.parse(str) || {};
        } catch (e) {
            cb(new InvalidAuthTokenError());
            return;
        }

        if (cracked.v !== 1 && cracked.v !== 2) {
            cb(new InvalidAuthTokenError('an invalid version'));
            return;
        }

        if ((Date.now() - cracked.t) > aes.maxAge) {
            cb(new InvalidAuthTokenError('expired'));
            return;
        }

        var obj;
        if (cracked.v === 1) {
            obj = {
                caller: {
                    roles: {},
                    account: {
                        uuid: cracked.u,
                        login: cracked.l,
                        groups: cracked.g,
                        approved_for_provisioning: true,
                        isOperator: cracked.g.some(function (e) {
                            return (e === 'operators');
                        })
                    }
                },
                ctime: cracked.t
            };
        } else if (cracked.v === 2) {
            obj = {
                principal: cracked.p,
                conditions: cracked.c,
                ctime: cracked.t
            };
        }

        cb(null, obj);
    });
}




///--- Handlers

function createAuthTokenHandler(req, res, next) {
    var aes = req.config.authToken;
    var caller = req.caller;
    var context = req.authContext;
    var log = req.log;

    var opts = {
        caller: caller,
        context: context,
        fromjob: false
    };

    log.debug(opts, 'createAuthToken: entered');
    createAuthToken(opts, aes, function (err, token) {
        if (err) {
            log.error(err, 'unable to create auth token');
            next(new InternalError());
            return;
        }

        // HAProxy has an 8k limit on header size
        if (Buffer.byteLength(token) > 8192) {
            log.error({token: token}, 'createAuthToken: token too big');
            next(new InternalError());
            return;
        }

        log.debug({token: token}, 'createAuthToken: done');
        res.send(201, {token: token});
        next();
    });
}


function checkIfPresigned(req, res, next) {
    if (req.headers.authorization ||
        (!req.query.expires &&
         !req.query.signature &&
         !req.query.keyId &&
         !req.query.algorithm)) {
        next();
    } else {
        req._presigned = true;
        next();
    }
}


function preSignedUrl(req, res, next) {
    if (!req.isPresigned()) {
        next();
        return;
    }

    var expires;
    var log = req.log;
    /* JSSTYLED */
    var methods = (req.query.method || req.method).split(/\s*,\s*/);
    var now = Math.floor(Date.now()/1000);

    methods.sort();

    log.debug('preSignedUrl: entered');

    if (methods.indexOf(req.method) === -1) {
        next(new PreSignedRequestError(req.method +
                                       ' was not a signed method'));
        return;
    }

    var missing = [
        'algorithm',
        'expires',
        'keyId',
        'signature'].filter(function (k) {
            return (!req.query[k]);
        });

    if (missing.length) {
        next(new PreSignedRequestError('parameters "' +
                                       missing.join(', ') +
                                       '" are required'));
        return;
    }

    try {
        expires = parseInt(req.query.expires, 10);
    } catch (e) {
        next(new PreSignedRequestError('expires is invalid'));
        return;
    }

    log.debug({
        expires: expires,
        now: now
    }, 'checking if request is  expired');
    if (now > expires) {
        next(new PreSignedRequestError('request expired'));
        return;
    }


    var parsed = {
        scheme: 'Signature',
        algorithm: req.query.algorithm.toUpperCase(),
        params: {
            keyId: req.query.keyId,
            signature: req.query.signature,
            role: req.query.role,
            'role-tag': req.query['role-tag']
        },
        signature: req.query.signature,
        signingString: ''
    };

    // Build the signing string, which is:
    // METHOD\n
    // $value_of_host_header
    // REQUEST_URL\n
    // key=val&...
    // with sorted query params (lexicographically),
    // minus the actual signature.
    parsed.signingString =
        methods.join(',') + '\n' +
        req.header('host') + '\n' +
        req.pathPreSanitize + '\n' +
        Object.keys(req.query).sort(function (a, b) {
            return (a.localeCompare(b));
        }).filter(function (k) {
            return (k.toLowerCase() !== 'signature');
        }).map(function (k) {
            return (rfc3986(k) + '=' + rfc3986(req.query[k]));
        }).join('&');

    log.debug({signatureOptions: parsed}, 'preSignedUrl: parsed');

    if (SIGN_ALG[parsed.algorithm] !== true) {
        next(new PreSignedRequestError(parsed.algorithm +
                                       ' is not a supported signing ' +
                                       'algorithm'));
        return;
    }

    req.auth = {
        role: req.query.role || '',
        'role-tag': req.query['role-tag'] || '',
        callerKey: req.query.keyId,
        signature: parsed
    };

    log.debug({auth: req.auth}, 'preSignedUrl: done');
    next();
}


function checkAuthzScheme(req, res, next) {
    if (!req.authorization.scheme) {
        req.log.debug('checkAuthzScheme: no auth scheme found');
        next();
        return;
    }

    var scheme = req.authorization.scheme.toLowerCase();
    var ok = ['signature', 'token'].indexOf(scheme) >= 0;

    if (!ok) {
        next(new AuthSchemeError(scheme));
    } else {
        next();
    }
}


function parseAuthTokenHandler(req, res, next) {
    if ((req.authorization.scheme || '').toLowerCase() !== 'token') {
        setImmediate(next);
        return;
    }

    req.log.debug('using token auth');
    req.log.debug('parseAuthTokenHandler: entered');

    var aes = req.config.authToken;
    var tkn = req.authorization.credentials;
    parseAuthToken(tkn, aes, function (err, token) {
        if (err) {
            req.log.debug(err, 'failed to crack token');
            next(new InvalidAuthTokenError());
            return;
        }

        req.auth = {
            accountid: token.principal.account.uuid,
            token: token
        };

        if (token.principal.user) {
            req.auth.userid = token.principal.user.uuid;
        }

        req.log.debug('parseAuthTokenHandler: done');
        next();
    });
}


function signatureHandler(req, res, next) {
    if ((req.authorization.scheme || '').toLowerCase() !== 'signature') {
        next();
        return;
    }

    req.log.debug('using signature auth');
    var algorithm = req.authorization.signature.algorithm;
    if (SIGN_ALG[algorithm] !== true) {
        next(new InvalidAlgorithmError(algorithm,
            Object.keys(SIGN_ALG).join(', ')));
        return;
    }

    req.auth = {
        signature: req.authorization.signature,
        callerKey: req.authorization.signature.keyId
    };
    next();
}


function parseKeyId(req, res, next) {
    if (!req.auth.callerKey) {
        next();
        return;
    }

    req.log.debug('parseKeyId: entered');
    var k;
    try {
        k = req.auth.callerKey.split('/');
    } catch (e) {
        next(new InvalidKeyIdError());
        return;
    }

    if (!k) {
        next(new InvalidKeyIdError());
        return;
    }

    if (k.length === 4) {
        // account key. like '/poseidon/keys/<keyId>'
        if (k[2] !== 'keys') {
            next(new InvalidKeyIdError());
            return;
        }
        req.auth.keyId = decodeURIComponent(k[3]);
        req.auth.account = decodeURIComponent(k[1]);
    } else if (k.length === 5) {
        // user key. like '/poseidon/fred/keys/<keyId>'
        if (k[3] !== 'keys') {
            next(new InvalidKeyIdError());
            return;
        }
        req.auth.keyId = decodeURIComponent(k[4]);
        req.auth.account = decodeURIComponent(k[1]);
        req.auth.user = decodeURIComponent(k[2]);
        if (req.auth.user === '') {
            next(new InvalidKeyIdError());
            return;
        }
    } else {
        next(new InvalidKeyIdError());
        return;
    }

    if (req.auth.keyId === '' || req.auth.account === '') {
        next(new InvalidKeyIdError());
        return;
    }

    req.log.debug('parseKeyId: done');
    next();
}


function loadCaller(req, res, next) {
    var account = req.auth.account;
    var accountid = req.auth.accountid;
    var user = req.auth.user;
    var userid = req.auth.userid;

    req.log.debug('loadCaller: entered');
    function gotCaller(err, info) {
        if (err) {
            switch (err.restCode || err.name) {
            case 'AccountDoesNotExist':
                next(new AccountDoesNotExistError(account));
                break;
            case 'UserDoesNotExist':
                next(new UserDoesNotExistError(account, user));
                break;

            /*
             * Technically these should never happen because uuids are only
             * used if we're using token auth, and tokens are generated by
             * muskie and we never delete users. Including them anyway in case
             * we ever do support deleting users.
             */
            case 'UserIdDoesNotExist':
                next(new UserDoesNotExistError(null, userid));
                break;
            case 'AccountIdDoesNotExist':
                next(new AccountDoesNotExistError(accountid));
                break;

            default:
                next(new InternalError(err));
                break;
            }
            return;
        }

        if (!info.account.approved_for_provisioning &&
            !info.account.isOperator) {
            next(new AccountBlockedError(info.account.login));
            return;
        }

        req.caller = info;
        req.log.debug({caller: req.caller}, 'loadCaller: done');
        next();
    }

    if (user && account) {
        req.mahi.getUser(user, account, false, gotCaller);
    } else if (userid) {
        req.mahi.getUserById(userid, gotCaller);
    } else if (account) {
        req.mahi.getAccount(account, gotCaller);
    } else if (accountid) {
        req.mahi.getAccountById(accountid, gotCaller);
    } else {
        // Caller did not supply enough identity information (if any).
        // Treat the request as an anonymous request.
        if (common.PUBLIC_STOR_PATH.test(req.path())) {
            // Objects under /public are always accessible by everyone
            // (regardless of anonymous user)
            req.caller = {
                roles: {},
                account: {}
            };
        } else {
            req.caller = {
                anonymous: true,
                user: {},
                roles: {},
                account: {}
            };
        }
        req.log.debug({caller: req.caller}, 'loadCaller: done');
        setImmediate(next);
        return;
    }
}


function verifySignature(req, res, next) {
    if (!req.auth.signature) {
        setImmediate(next);
        return;
    }

    req.log.debug('verifySignature: entered');

    var user = req.caller.user;
    var account = req.caller.account;

    var keyId = req.auth.keyId;
    var signature = req.auth.signature;

    var keys = user ? user.keys : account.keys;
    if (!keys || !keys[keyId]) {
        next(new KeyDoesNotExistError(
            account.login,
            keyId,
            user ? user.login : null));
        return;
    }

    var key = keys[keyId];
    try {
        var ok = httpSignature.verifySignature(signature, key);
    } catch (e) {
        next(new InternalError(e));
        return;
    }
    if (!ok) {
        next(new InvalidSignatureError());
        return;
    }

    req.log.debug('verifySignature: done');
    next();
}


function parseHttpAuthToken(req, res, next) {
    if (!req.header('x-auth-token')) {
        next();
        return;
    }

    var log = req.log;
    var token;

    try {
        token = JSON.parse(req.header('x-auth-token'));
    } catch (e) {
        log.warn(e, 'invalid auth token (JSON parse)');
        next(new InvalidHttpAuthTokenError('malformed auth token'));
        return;
    }

    log.debug('parseHttpAuthToken: calling keyAPI');
    req.keyapi.detoken(token, function (tokerr, tokobj) {

        function gotInfo(err, info) {
            if (err) {
                switch (err.restCode) {
                case 'AccountDoesNotExist':
                    next(new AccountDoesNotExistError(req.auth.account));
                    return;
                case 'UserDoesNotExist':
                    next(new UserDoesNotExistError(req.auth.account,
                            req.auth.user));
                    return;
                default:
                    next(new InternalError(err));
                    return;
                }
            }

            req.caller = info;
            log.debug(req.auth.account, 'parseHttpAuthToken: done');
            next();
        }

        if (tokerr || !tokobj) {
            log.warn(tokerr, 'invalid auth token (detoken)');
            next(new InvalidHttpAuthTokenError('malformed auth token'));
        } else if (tokobj.expires &&
                   (Date.now() > new Date(tokobj.expires).getTime())) {
            next(new InvalidHttpAuthTokenError('auth token expired'));
        } else if (!req.authorization || !req.authorization.signature ||
                   !req.authorization.signature.keyId) {
            next(new AuthorizationRequiredError('signature is required'));
        } else if (tokobj.devkeyId !== req.authorization.signature.keyId) {
            next(new InvalidHttpAuthTokenError('not authorized for token'));
        } else {
            req.auth.delegate = req.auth.account;
            req.auth.account = tokobj.account.login;
            if (tokobj.subuser) {
                req.auth.user = tokobj.subuser.login;
                req.mahi.getUser(req.auth.user, req.auth.account, gotInfo);
            } else {
                req.mahi.getAccount(req.auth.account, gotInfo);
            }
        }
    });
}


function loadOwner(req, res, next) {
    var p = req.path();
    loadOwnerFromPath(req, p, next);
}


/*
 * Extract the owner of a resource based on the input path, verify that
 * the account exists, and set the `owner` field on the request object
 * to the object returned from Mahi.
 */
function loadOwnerFromPath(req, p, next) {
    req.log.debug('loadOwner: entered');

    var account;
    try {
        account = decodeURIComponent(p.split('/', 2).pop());
    } catch (e) {
        next(new InvalidPathError(p));
        return;
    }

    req.auth.owner = account;
    var user = common.ANONYMOUS_USER;
    var fallback = true;

    req.mahi.getUser(user, account, fallback, function (err, owner) {
        if (err) {
            switch (err.restCode || err.name) {
            case 'AccountDoesNotExist':
                next(new AccountDoesNotExistError(account));
                return;
            default:
                next(new InternalError(err));
                return;
            }
        }

        req.owner = owner;

        if (req.caller.anonymous && !owner.user) {
            next(new AuthorizationError(common.ANONYMOUS_USER, p,
                'owner ' + account + ' has no anonymous user'));
            return;
        } else if (req.caller.anonymous) {
            req.log.debug('loadOwner: using owner\'s anonymous user');
            req.caller.account = owner.account;
            req.caller.user = owner.user;
            req.caller.roles = owner.roles;
        }

        if (!owner.account.approved_for_provisioning &&
            !owner.account.isOperator &&
            (req.caller.user || !req.caller.account.isOperator)) {

            next(new AccountBlockedError(account));
            return;
        }

        req.log.debug('loadOwner: done');
        next();
    });
}


/*
 * Determine the active roles for the request.
 * If the request used token auth, roles from the token override any other
 * requested roles (from headers, presigned URL). The token is created by
 * muskie so the roles in the token have already been verified.
 * Then, check the roles from the URL or headers, if present (with ones from the
 * URL taking precedence).
 */
function getActiveRoles(req, res, next) {
    if (req.auth.token && req.auth.token.conditions) {
        req.activeRoles = req.auth.token.conditions.activeRoles || [];
        setImmediate(next);
        return;
    }

    var requestedRoles;

    if (req.auth && typeof (req.auth.role) === 'string') { // from URL
        requestedRoles = req.auth.role;
    } else {
        requestedRoles = req.headers['role'];
    }

    var activeRoles = [];
    var names;

    if (requestedRoles) {           // The user passed in roles to assume
        if (requestedRoles  === '*' && req.caller.user) {
            activeRoles = req.caller.user.roles || [];
            req.activeRoles = activeRoles;
            req.authContext.conditions.activeRoles = activeRoles;
            setImmediate(next);

            return;
        }

        /* JSSTYLED */
        names = requestedRoles.split(/\s*,\s*/);
        req.mahi.getUuid({
            account: req.caller.account.login,
            type: 'role',
            names: names
        }, function (err, lookup) {
            if (err) {
                next(new InternalError(err));
                return;
            }
            var i, uuid;
            for (i = 0; i < names.length; i++) {
                uuid = lookup.uuids[names[i]];
                if (!uuid || !req.caller.roles[uuid]) {
                    next(new InvalidRoleError(names[i]));
                    return;
                }
                activeRoles.push(uuid);
            }
            req.activeRoles = activeRoles;
            next();
        });
    } else {                            // No explicit roles, use default set
        if (req.caller.user) {
            activeRoles = req.caller.user.defaultRoles || [];
        }
        req.activeRoles = activeRoles;
        setImmediate(next);
    }
}


function gatherContext(req, res, next) {
    var action = req.route.authAction || req.route.name;

    var conditions = req.authContext.conditions;
    conditions.owner = req.owner.account;
    conditions.method = req.method;
    conditions.activeRoles = req.activeRoles;
    conditions.date = new Date(req._time);
    conditions.day = new Date(req._time);
    conditions.time = new Date(req._time);
    var ip = req.headers['x-forwarded-for'];
    if (ip) {
        conditions.sourceip = ip.split(',')[0].trim();
    }
    conditions['user-agent'] = req.headers['user-agent'];
    conditions.fromjob = false;

    // Override conditions with ones that are provided in the token
    if (req.auth.token) {
        Object.keys(req.auth.token.conditions).forEach(function (k) {
            conditions[k] = req.auth.token.conditions[k];
        });
    }

    req.authContext.principal = req.caller;
    req.authContext.action = action.toLowerCase();
    next();
}


function storageContext(req, res, next) {
    var resource = {};
    var metadata;

    /*
     * XXX No type and no parent metadata means this is a GET of a nonexisting
     * object. Ideally if the caller has read access to the parent directory, we
     * would return a 404, and if the caller does not, we would return a 403.
     * However, since we do not fetch the parent directory's metadata on GETs
     * (for performance), we don't know whether the caller does or does not have
     * read access on the parent directory. So we will return a 404 regardless.
     *
     * This means that there is a information disclosure vulnerability since
     * we leak information about the existence of objects. If this becomes an
     * issue, we would want to add a step here to go and get the parent
     * directory's metadata and check the roles to see whether we should return
     * a 403 or 404.
     */
    if (!req.metadata.type && !req.parentMetadata) {
        next(new ResourceNotFoundError(req.path()));
        return;
    }

    /*
     * If parentMetadata exists, then this is a PUT request. In this case both
     * metadata.type and parentMetadata.type are null. This means that the
     * parent directory does not exist. More parent directory checks also occur
     * in PUT handlers, but we check this specific case here to short-circuit
     * authorization checks and return the correct error.
     */
    if (!req.metadata.type && !req.parentMetadata.type) {
        next(new DirectoryDoesNotExistError(req.path()));
        return;
    }

    if (!req.metadata.type && req.parentMetadata.type) { // PUT new obj or dir
        metadata = req.parentMetadata;
        req.authContext.conditions.overwrite = false;
    } else { // PUT on existing obj or dir
        metadata = req.metadata;
        req.authContext.conditions.overwrite = true;
    }

    resource.owner = req.owner;
    resource.key = metadata.key || req.key;
    resource.roles = metadata.roles || [];

    req.authContext.resource = resource;

    /*
     * GET, HEAD and DELETE routes handle both objects and directories, so
     * authAction can't be set at the route level. Since the authorization
     * handler runs before the object or directory handler, we have to use the
     * metadata to determine the action. Overwrite the existing authAction with
     * the appropriate one here (instead of using the route name).
     *
     * We don't have to worry about PUTs because each kind of PUT has its own
     * route (and authAction will be set).
     */
    if (!req.route.authAction) {
        switch (req.method) {
        case 'GET':
            /* falls through */
        case 'HEAD':
            req.authContext.action = 'get' + req.metadata.type;
            break;
        case 'DELETE':
            req.authContext.action = 'delete' + req.metadata.type;
            break;
        default:
            /* default to route name from gatherContext */
            break;
        }
    }

    next();
}


function authorize(req, res, next) {
    var log = req.log;

    log.debug({caller: req.caller, owner: req.owner}, 'authorize: entered');

    var login;

    if (!req.caller.user) {
        login = req.caller.account.login;
    } else {
        login = req.caller.account.login + '/' + req.caller.user.login;
    }

    req.log.debug(req.authContext, 'authorizing...');

    try {
        libmanta.authorize({
            mahi: req.mahi,
            context: req.authContext
        });
    } catch (e) {
        switch (e.restCode || e.name) {
        case 'AccountBlocked':
            next(new AccountBlockedError(req.caller.account.login));
            return;
        case 'NoMatchingRoleTag':
            next(new NoMatchingRoleTagError());
            return;
        case 'InvalidRole':
            next(new InvalidRoleError(e.message));
            return;
        case 'CrossAccount':
            next(new AuthorizationError(login, req.path(), e));
            return;
        case 'RulesEvaluationFailed':
            next(new AuthorizationError(login, req.path(), e));
            return;
        default:
            if (e.statusCode >= 400 && e.statusCode <= 499) {
                next(new AuthorizationError(login, req.path(), e));
                return;
            }
            return (next(new InternalError(e)));
        }
    }

    next();
}



///--- Exports

module.exports = {

    authenticationHandler: function handlers(options) {
        assert.object(options, 'options');
        assert.object(options.log, 'options.log');
        assert.func(options.mahi, 'options.mahi');

        return ([
            function _authSetup(req, res, next) {
                req.mahi = options.mahi();
                req.keyapi = options.keyapi();
                req.auth = {};
                req.authContext = {
                    conditions: {}
                };
                next();
            },
            preSignedUrl,
            checkAuthzScheme,
            parseAuthTokenHandler,
            signatureHandler,
            parseKeyId,
            loadCaller,
            verifySignature,
            parseHttpAuthToken,
            loadOwner,
            getActiveRoles
        ]);
    },

    authorizationHandler: function authz() {
        return ([
            authorize
        ]);
    },

    loadOwnerFromPath: loadOwnerFromPath,

    gatherContext: gatherContext,
    storageContext: storageContext,
    createAuthToken: createAuthToken,
    parseAuthToken: parseAuthToken,
    checkIfPresigned: checkIfPresigned,

    postAuthTokenHandler: function () {
        return ([createAuthTokenHandler]);
    }
};
