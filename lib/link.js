/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var url = require('url');

var assert = require('assert-plus');
var libmanta = require('libmanta');
var libuuid = require('libuuid');
var restify = require('restify');
var vasync = require('vasync');

var common = require('./common');
var sharkClient = require('./shark_client');
var utils = require('./utils');
require('./errors');

var clone = utils.shallowCopy;



///--- Helpers

// From restify
function sanitizePath(p) {

    // Be nice like apache and strip out any //my//foo//bar///blah
    p = p.replace(/\/\/+/g, '/');

    // Kill a trailing '/'
    if (p.lastIndexOf('/') === (p.length - 1) && p.length > 1)
        p = p.substr(0, p.length - 1);

    return (p);
}



function sharkLink(opts, cb) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.sharkAgent, 'opts.sharkAgent');
    assert.object(opts.sharkConfig, 'opts.sharkConfig');
    assert.optionalNumber(opts.sharkConfig.connectTimeout,
        'opts.sharkConfig.connectTimeout');
    assert.optionalObject(opts.sharkConfig.retry, 'opts.sharkConfig.retry');
    assert.object(opts.shark, 'opts.shark');

    var client = sharkClient.getClient({
        connectTimeout: opts.sharkConfig.connectTimeout,
        log: opts.log,
        retry: opts.sharkConfig.retry,
        shark: opts.shark,
        agent: opts.sharkAgent
    });
    assert.ok(client, 'sharkClient should have been created');

    opts.log.debug({shark: opts.shark}, 'sharkLink entered');

    client.putLink(opts, function (err, req, res) {
        if (err) {
            cb(err);
        } else {
            req._shark = opts.shark;

            //
            // It's not clear what this would be, so if we hit a non-error
            // non-204 we want to abort.
            //
            // Note: until MANTA-4410 is fixed, this doesn't actually abort.
            //
            assert.equal(res.statusCode, 204, 'expected 204');

            opts.log.debug({
                req: req,
                statusCode: res.statusCode
            }, 'SharkClient: putLink');

            cb(null, req);
        }
    });
}



///--- Routes
//
// We only have put handlers, as once a link is written in the system all the
// object APIs "just work". (note that on creating a new link, we actually set
// the type to 'object' -> you can distinguish that an object was created via
// link by checking the createdFrom attribute in moray).
//


//-- PUT Handlers --//

function parseLocation(req, res, next) {
    if (!req.headers.location)
        return (next(new LinkRequiredError()));

    req.link = {
        owner: {}
    };

    try {
        req.link.path =
            sanitizePath(url.parse(req.headers.location).pathname);

        var re;

        if (common.STOR_PATH.test(req.link.path)) {
            re = common.STOR_PATH;
        } else if (common.PUBLIC_STOR_PATH.test(req.link.path)) {
            re = common.PUBLIC_STOR_PATH;
        } else if (common.JOBS_STOR_PATH.test(req.link.path)) {
            re = common.JOBS_STOR_PATH;
        } else if (common.REPORTS_STOR_PATH.test(req.link.path)) {
            re = common.REPORTS_STOR_PATH;
        }

        if (re) {
            var params = re.exec(req.link.path);
            req.link.owner.login = decodeURIComponent(params[1]);
        }

    } catch (e) {
        return (next(new InvalidLinkError(req)));
    }

    req.log.debug({
        link: req.link
    }, 'parseLocation: done');
    next();
}


function resolveOwner(req, res, next) {
    var log = req.log;
    var login = req.link.owner.login;

    log.debug('link.resolveOwner: entered');
    req.mahi.getUser(common.ANONYMOUS_USER, login, true, function (err, info) {
        if (err) {
            switch (err.restCode) {
            case 'AccountDoesNotExist':
                next(new LinkNotFoundError(req));
                return;
            default:
                next(new InternalError(err));
                return;
            }
        }
        req.link.owner = info;
        var _opts = {
            account: req.link.owner.account,
            path: req.link.path
        };
        libmanta.normalizeMantaPath(_opts, function (err2, p) {
            if (err2) {
                req.log.debug(err2, 'Invalid link');
                next(new InvalidLinkError(req));
                return;
            }
            req.link.key = p;
            log.debug({
                link: req.link,
                owner: req.link.owner
            }, 'link.resolveOwner: done');
            next();
        });
    });
}


function ensureSourceOwnerSnaplinksEnabled(req, res, next) {
    var log = req.log;
    var uuid = req.link.owner.account.uuid;

    common.checkAccountSnaplinksEnabled(req, uuid, function (enabled) {
        if (!enabled) {
            log.debug({
                link: req.link,
                owner: req.link.owner
            }, 'link.ensureSourceOwnerSnaplinksEnabled: source owner ' +
                'has snaplinks disabled');
            next(new SnaplinksDisabledError('owner of source object has ' +
                'snaplinks disabled'));
            return;
        }
        next();
    });
}


function resolveSource(req, res, next) {
    var log = req.log;
    var opts = {
        key: req.link.key,
        requestId: req.getId()
    };

    log.debug({link: req.link}, 'resolveSource: entered');

    common.loadMetadata(req, opts, function (err, md) {
        if (err) {
            next(err);
        } else if (!md || md.type === null) {
            return (next(new LinkNotFoundError(req)));
        } else if (md.type !== 'object') {
            return (next(new LinkNotObjectError(req)));
        } else {
            req.link.metadata = md;

            // Conditional request needs these
            res.set('Etag', md.etag);
            if (md.mtime)
                res.set('Last-Modified', new Date(md.mtime));

            log.debug({link: req.link}, 'resolveSource: done');
            next();
        }
    });
}


function checkAccess(req, res, next) {
    req.log.debug({
        caller: req.caller,
        target: req.owner,
        linkSource: req.link,
        path: req.path()
    }, 'link.authorize: entered');

    req.authContext.action = 'getobject';
    req.authContext.resource = {
        owner: req.link.owner,
        key: req.link.key,
        roles: req.link.metadata.roles
    };

    try {
        libmanta.authorize({
            mahi: req.mahi,
            context: req.authContext
        });
    } catch (e) {
        next(new AuthorizationError(req.owner.account.login, req.link.path, e));
        return;
    }

    req.log.debug('link.authorize: ok');
    next();
}


/*
 * This function will:
 *
 *  - create a new Object Id
 *  - attempt to PUT the new link on all mako/shark/storage zones
 *  - if any of the PUTs failed, add an entry to the manta_fastdelete_queue for
 *    the failed (new) object.
 *
 */
function createSnapLink(req, res, next) {
    var opts = {
        contentType: req.getContentType(),
        owner: req.owner.account.uuid,
        requestId: req.getId(),
        sharkAgent: req.sharkAgent,
        sharkConfig: req.sharkConfig,
        sourceCreator: req.link.metadata.creator || req.link.metadata.owner,
        sourceObjectId: req.link.metadata.objectId
    };

    req.newObjectId = libuuid.create();
    opts.objectId = req.newObjectId;

    assert.object(req.link, 'req.link');
    assert.object(req.link.metadata, 'req.link.metadata');
    assert.arrayOfObject(req.link.metadata.sharks, 'req.link.metadata.sharks');
    assert.uuid(opts.objectId, 'options.objectId');
    assert.uuid(opts.owner, 'options.owner');
    assert.uuid(opts.sourceCreator, 'options.sourceCreator');
    assert.uuid(opts.sourceObjectId, 'options.sourceObjectId');

    vasync.forEachParallel({
        func: function shark_link(shark, cb) {
            var _opts = clone(opts);
            _opts.log = req.log;
            _opts.shark = shark;

            sharkLink(_opts, cb);
        },
        inputs: req.link.metadata.sharks
    }, function (err, results) {
        req.sharks = [];

        if (results.successes) {
            req.sharks = results.successes.map(function _mshark(s) {
                return (s._shark);
            });
        }

        req.log[err ? 'error' : 'debug']({
            err: err,
            ndone: results.ndone,
            nerrors: results.nerrors,
            sharks: req.sharks
        }, 'done sharkLinking');

        if (err) {
            //
            // Note:
            //
            //  Like PutObject, it's possible that we'll fail and leave data
            //  (new hard links) on the storage zones. Also like PutObject,
            //  these are invisible to the metadata tier so will need a separate
            //  process to clean up.
            //
            //  In the future, we should add such objects to
            //  manta_fastdelete_queue or equivalent so that they're cleaned up.
            //  See MANTA-4286 for description of the problem in the PutObject
            //  case.
            //
            next(new InternalError(err));
            return;
        }

        // We've made the links, now we need to add the correct metadata entries
        // which will happen in saveMetadata().
        next();
    });
}


function saveMetadata(req, res, next) {
    var log = req.log;

    common.createMetadata(req, 'link', function (err, opts) {
        if (err) {
            next(err);
            return;
        }

        opts.creator = req.link.metadata.creator || req.link.metadata.owner;
        opts.previousMetadata = req.metadata;
        opts.newObjectId = req.newObjectId;

        log.debug(opts, 'saveMetadata: entered');

        req.moray.putMetadata(opts, function (err2) {
            if (err2) {
                log.debug(err2, 'saveMetadata: failed');
                next(err2);
            } else {
                var lmd = req.link.metadata;
                log.debug('saveMetadata: done');
                res.header('Etag', lmd.etag);
                res.header('Last-Modified', new Date(lmd.mtime));
                res.send(204);
                next();
            }
        });
    });
}



///--- Exports

module.exports = {

    putLinkHandler: function () {
        var chain = [
            common.ensureSnaplinksEnabledHandler(),
            common.ensureNotRootHandler(),
            common.ensureNotDirectoryHandler(),
            common.ensureParentHandler(),
            parseLocation,
            resolveOwner,
            ensureSourceOwnerSnaplinksEnabled,
            resolveSource,
            checkAccess,
            restify.conditionalRequest(),
            createSnapLink,
            saveMetadata
        ];
        return (chain);
    }

};
