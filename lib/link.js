/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
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
 * New objects in Manta have a boolean marker field called `singlePath` in their
 * metadata records. This field's presence indicates that the object has only
 * ever had a single reference in the metadata tier. If the object we are
 * attempting to link to has the `singlePath` field, we must clear it before
 * writing out the 'link' metadata so that inbound deletes to this object do
 * not default to accelerated garbage-collection.
 *
 * It is critical that Muskie update this field on the source object metadata
 * before writing out the link metadata. If Muskie cleared the field after
 * writing out the link metadata, a delete for the source object occuring after
 * Muskie writes out the link metadata, but before Muskie clears the
 * `singlePath` field on the source object metadata, would result in the source
 * object being incorrectly garbage-collected with accelerated
 * garbage-collection.
 *
 * Accelerated garbage-collection is not snaplink-aware, therefore the 'link'
 * metadata created by this request would become a dangling reference.
 *
 * The disadvantage of clearing the `singlePath` field on the source object
 * before writing out the 'link' metadata is that Muskie may subsequently fail
 * while writing out the 'link' metadata. In this case we've cleared the
 * `singlePath` field on an object that still only has one reference in the
 * metadata-tier, making the source object ineligible for accelerated
 * garbage-collection unnecessarily.
 */
function clearSinglePathFieldIfPresent(req, res, next) {
    var log = req.log;
    var singlePath = 'singlePath';
    var sourceMetadata = req.link.metadata;

    /*
     * It is crucial that we do not check `req.singlePathEnabled` here. If we
     * did, there would be no way to do a rolling restart of the Muskies in a
     * datacenter without creating a window in which an updated Muskie can
     * create a new object with a `singlePath` field set to true, and an "old"
     * Muskie can handle a snaplink request for the same object that does not
     * clear the `singlePath` field.
     */
    if (!sourceMetadata.hasOwnProperty(singlePath) ||
        !sourceMetadata.singlePath) {
        next();
        return;
    }

    /*
     * For future debugging purposes, we set this field to false instead of
     * deleting it. This way, we can distinguish between objects that have never
     * had the `singlePath` field and objects that did, at one point, have it.
     */
    sourceMetadata.singlePath = false;
    sourceMetadata.requestId = req.getId();
    sourceMetadata._etag = req.isConditional() ? req.link.metadata._etag :
        undefined;

    log.debug(sourceMetadata, 'clearSinglePathIfPresent: entered');
    req.moray.putMetadata(sourceMetadata, function (err) {
        if (err)
            log.debug(err, 'clearSinglePathIfPresent: failed');
        next(err);
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
            clearSinglePathFieldIfPresent,
            saveMetadata
        ];
        return (chain);
    }

};
