/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var url = require('url');

var libmanta = require('libmanta');
var restify = require('restify');

var common = require('./common');
require('./errors');



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


function saveMetadata(req, res, next) {
    var log = req.log;
    common.createMetadata(req, 'link', function (err, opts) {
        if (err) {
            next(err);
            return;
        }

        opts.creator = req.link.metadata.creator || req.link.metadata.owner;
        opts.previousMetadata = req.metadata;

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
            common.ensureNotRootHandler(),
            common.ensureNotDirectoryHandler(),
            common.ensureParentHandler(),
            parseLocation,
            resolveOwner,
            resolveSource,
            checkAccess,
            restify.conditionalRequest(),
            saveMetadata
        ];
        return (chain);
    }

};
