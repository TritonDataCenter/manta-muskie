/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var EventEmitter = require('events').EventEmitter;
var http = require('http');
var os = require('os');
var path = require('path');
var util = require('util');
var httpSignature = require('http-signature');

var assert = require('assert-plus');
var libmanta = require('libmanta');
var morayFilter = require('moray-filter');
var vasync = require('vasync');
var restifyErrors = require('restify-errors');
var VError = require('verror');

var muskieUtils = require('./utils');

require('./errors');



///--- Globals

var sprintf = util.format;

var ANONYMOUS_USER = libmanta.ANONYMOUS_USER;

var CORS_RES_HDRS = [
    'access-control-allow-headers',
    'access-control-allow-origin',
    'access-control-expose-headers',
    'access-control-max-age',
    'access-control-allow-methods'
];

/* JSSTYLED */
var UPLOADS_ROOT_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\.@%]+)\/uploads\/?.*/;
/* JSSTYLED */
var PUBLIC_STOR_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\-\.@%]+)\/public(\/(.*)|$)/;
var REPORTS_STOR_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\-\.@%]+)\/reports(\/(.*)|$)/;
var STOR_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\-\.@%]+)\/stor(\/(.*)|$)/;

// Thanks for being a PITA, javascriptlint (it doesn't like /../ form in [])
var ROOT_REGEXPS = [
    new RegExp('^\\/[a-zA-Z0-9_\\-\\.@%]+$'), // /:login
    new RegExp('^\\/[a-zA-Z0-9_\\-\\.@%]+\\/public\\/?$'), // public
    new RegExp('^\\/[a-zA-Z0-9_\\-\\.@%]+\\/stor\\/?$'), // storage
    new RegExp('^\\/[a-zA-Z0-9_\\-\\.@%]+\\/uploads\\/?$'), // uploads (list)

    new RegExp('^\\/[a-zA-Z0-9_\\-\\.@%]+\\/reports\\/?$') // reports
];

var PATH_LOGIN_RE = /^\/([a-zA-Z][a-zA-Z0-9_\-\.@%]+)\//;

var ZONENAME = os.hostname();

// Names of metric collectors.
var METRIC_REQUEST_COUNTER = 'http_requests_completed';
var METRIC_LATENCY_HISTOGRAM = 'http_request_latency_ms';
var METRIC_DURATION_HISTOGRAM = 'http_request_time_ms';
var METRIC_INBOUND_DATA_COUNTER = 'muskie_inbound_streamed_bytes';
var METRIC_OUTBOUND_DATA_COUNTER = 'muskie_outbound_streamed_bytes';
var METRIC_DELETED_DATA_COUNTER = 'muskie_deleted_bytes';

// The max number of headers we store on an object in Moray: 4 KB.
var MAX_HDRSIZE = 4 * 1024;

///--- Internals


///--- Patches

var HttpRequest = http.IncomingMessage.prototype; // save some chars

HttpRequest.abandonSharks = function abandonSharks() {
    var self = this;
    (this.sharks || []).forEach(function (shark) {
        shark.removeAllListeners('result');
        shark.abort();
        self.unpipe(shark);
    });
};


HttpRequest.isConditional = function isConditional() {
    return (this.headers['if-match'] !== undefined ||
            this.headers['if-none-match'] !== undefined);
};


HttpRequest.isPresigned = function isPresigned() {
    return (this._presigned);
};


HttpRequest.isPublicGet = function isPublicGet() {
    var ok = this.isReadOnly() && PUBLIC_STOR_PATH.test(this.path());

    return (ok);
};


HttpRequest.isPublicPut = function isPublicPut() {
    return (this.method === 'PUT' && PUBLIC_STOR_PATH.test(this.path()));
};


HttpRequest.isReadOnly = function isReadOnly() {
    var ro = this.method === 'GET' ||
        this.method === 'HEAD' ||
        this.method === 'OPTIONS';

    return (ro);
};


HttpRequest.isRootDirectory = function isRootDirectory(d) {
    function _test(dir) {
        var matches = ROOT_REGEXPS.some(function (re) {
            return (re.test(dir));
        });

        return (matches);
    }


    if (!d) {
        if (this._isRoot === undefined)
            this._isRoot = _test(this.path());

        return (this._isRoot);
    }

    return (_test(d));
};


HttpRequest.isRestrictedWrite = function isRestrictedWrite() {
    if (this.method !== 'PUT')
        return (false);

    var p = this.path();
    return (REPORTS_STOR_PATH.test(p));
};



///--- API

function createMetadata(req, type, cb) {
    var prev = req.metadata || {};
    /*
     * Override the UpdateMetadata type, as this flows in via PUT Object path.
     */
    if (prev.type === 'directory')
        type = 'directory';

    var names;
    var md = {
        dirname: path.dirname(req.key),
        key: req.key,
        headers: {},
        mtime: Date.now(),
        owner: req.owner.account.uuid,
        requestId: req.getId(),
        roles: [],
        type: type,
        // _etag is the moray etag, not the user etag
        // Note that we only specify the moray etag if the user sent
        // an etag on the request, otherwise, it's race time baby!
        // (on purpose) - note that the indexing ring will automatically
        // retry (once) on putMetadata if there was a Conflict Error and
        // no _etag was sent in
        _etag: req.isConditional() ? req.metadata._etag : undefined
    };

    CORS_RES_HDRS.forEach(function (k) {
        var h = req.header(k);
        if (h) {
            md.headers[k] = h;
        }
    });
    if (req.headers['cache-control'])
        md.headers['Cache-Control'] = req.headers['cache-control'];

    if (req.headers['surrogate-key'])
        md.headers['Surrogate-Key'] = req.headers['surrogate-key'];

    var hdrSize = 0;
    Object.keys(req.headers).forEach(function (k) {
        if (/^m-\w+/.test(k)) {
            hdrSize += Buffer.byteLength(req.headers[k]);
            if (hdrSize < MAX_HDRSIZE)
                md.headers[k] = req.headers[k];
        }
    });

    switch (type) {
    case 'directory':
        break;

    case 'link':
        md.link = req.link.metadata;
        break;

    case 'object':
        muskieUtils.validateContentDisposition(
            req.headers, function cdcb(err, _h) {
                if (err) {
                    req.log.debug('malformed content-disposition: %s', err.msg);
                    cb(new restifyErrors.BadRequestError());
                }
            });

        md.contentDisposition = req.header('content-disposition');
        md.contentLength = req._size !== undefined ?
            req._size : prev.contentLength;
        md.contentMD5 = req._contentMD5 || prev.contentMD5;
        md.contentType = req.header('content-type') ||
            prev.contentType ||
            'application/octet-stream';
        md.objectId = req.objectId || prev.objectId;
        if (md.contentLength === 0) { // Chunked requests
            md.sharks = [];
        } else if (req.sharks && req.sharks.length) { // Normal requests
            md.sharks = req.sharks.map(function (s) {
                return ({
                    datacenter: s._shark.datacenter,
                    manta_storage_id: s._shark.manta_storage_id
                });
            });
        } else { // Take from the prev is for things like mchattr
            md.sharks = (prev.sharks || []);
        }
        break;

    default:
        break;
    }

    // mchattr
    var requestedRoleTags;
    if (req.auth && typeof (req.auth['role-tag']) === 'string') { // from URL
        requestedRoleTags = req.auth['role-tag'];
    } else {
        requestedRoleTags = req.headers['role-tag'];
    }

    if (requestedRoleTags) {
        /* JSSTYLED */
        names = requestedRoleTags.split(/\s*,\s*/);
        req.mahi.getUuid({
            account: req.owner.account.login,
            type: 'role',
            names: names
        }, function (err, lookup) {
            if (err) {
                cb(err);
                return;
            }
            var i;
            for (i = 0; i < names.length; i++) {
                if (!lookup.uuids[names[i]]) {
                    cb(new InvalidRoleTagError(names[i]));
                    return;
                }
                md.roles.push(lookup.uuids[names[i]]);
            }
            cb(null, md);
        });
    // apply all active roles if no other roles are specified
    } else if (req.caller.user) {
        md.roles = req.activeRoles;
        setImmediate(function () {
            cb(null, md);
        });
    } else {
        setImmediate(function () {
            cb(null, md);
        });
    }
}


function assertMetadata(req, res, next) {
    if (!req.metadata || !req.metadata.type) {
        next(new ResourceNotFoundError(req.getPath()));
    } else {
        next();
    }
}


function enforceSSL(req, res, next) {
    if (!req.isSecure() && !req.isPresigned() && !req.isPublicGet()) {
        next(new SSLRequiredError());
    } else {
        next();
    }
}


function ensureEntryExists(req, res, next) {
    if (!req.metadata || req.metadata.type === null) {
        next(new ResourceNotFoundError(req.path()));
    } else {
        next();
    }
}


/*
 * This function is used to abstract away the logic involved in checking
 * that a particular account uuid potentially has existing SnapLinks.
 * An account is assumed to have no SnapLinks if:
 *
 *  * they are in the accountsSnaplinksDisabled array; or
 *  * we have completed the audit and "fixed" the system so that there are no
 *    longer *any* SnapLinks for any account. If we upgraded from Manta v1, the
 *    upgrade will have set "SNAPLINK_CLEANUP_REQUIRED". When that is gone, the
 *    cleanup has been completed.
 *
 * in either case, next() will be called with `false`. Otherwise next() will be
 * called with `true`.
 */
function checkAccountSnaplinksMightExist(req, uuid, next) {
    if (req.config.snaplinkCleanupRequired !== true) {
        //
        // We've run the cleanup and this Manta is SnapLink-free, so SnapLinks
        // will not exist for this account.
        //
        // Alternatively, this DC was setup with Manta v2 initially in which
        // case SnapLinks will also not exist for this account.
        //
        next(false);
        return;
    }

    for (var i = 0; i < req.accountsSnaplinksDisabled.length; i++) {
        var account = req.accountsSnaplinksDisabled[i];
        assert.string(account.uuid, 'account.uuid');

        if (account.uuid === uuid) {
            next(false);
            return;
        }
    }

    next(true);
}


function ensureNotDirectory(req, res, next) {
    if (!req.metadata) {
        next(new DirectoryOperationError(req));
    } else if (req.metadata.type === 'directory') {
        if (req.metadata.marlinSpoof || req.query.metadata) {
            /*
             * This is either a metadata update or a request from a Marlin
             * proxy, so we allow the directory to be overwritten.
             */
            next();
        } else {
            next(new DirectoryOperationError(req));
        }
    } else {
        next();
    }
}


function ensureNotRoot(req, res, next) {
    if (!req.isRootDirectory()) {
        next();
        return;
    }

    if (req.method === 'PUT') {
        if (req.headers['content-type'] && req.headers['content-type'] !==
            'application/x-json-stream; type=directory') {
            next(new RootDirectoryError(req.method, req.path()));
            return;
        }
    }

    if (req.method === 'DELETE') {
        next(new RootDirectoryError(req.method, req.path()));
        return;
    }

    next();
}


function ensureParent(req, res, next) {
    req.log.debug({
        parentKey: req.parentKey,
        parentMetadata: req.parentMetadata
    }, 'ensureParent: entered');

    if (req.isRootDirectory() || req.isRootDirectory(req.parentKey)) {
        req.log.debug('ensureParent: done');
        next();
    } else if (!req.parentMetadata || req.parentMetadata.type === null) {
        next(new DirectoryDoesNotExistError(req.path()));
    } else if (req.parentMetadata.type !== 'directory') {
        next(new ParentNotDirectoryError(req));
    } else {
        req.log.debug('ensureParent: done');
        next();
    }
}


/*
 * Resolves the metadata for the entry at req.key, and the entry's parent
 * metadata, if necessary.
 */
function getMetadata(req, res, next) {
    assert.ok(req.key);

    var log = req.log;
    log.debug('getMetadata: entered');
    vasync.parallel({
        funcs: [
            function entryMD(cb) {
                var opts = {
                    key: req.key,
                    requestId: req.getId()
                };
                loadMetadata(req, opts, function (err, md, w) {
                    if (err) {
                        cb(err);
                    } else {
                        var obj = {
                            op: 'entry',
                            metadata: md,
                            etag: (w || {})._etag
                        };

                        if (w && w._node) {
                            obj.shard = w._node.pnode;
                        }

                        cb(null, obj);
                    }
                });
            },
            // This is messy, but basically we don't resolve
            // the parent when we don't need to, but we want to
            // run in parallel when we do. So here we have some
            // funky logic to check when we don't.  It's some
            // sweet jack-hackery.
            function parentMD(cb) {
                if (req.method === 'GET' ||
                    req.method === 'HEAD' ||
                    req.method === 'DELETE' ||
                    req.isRootDirectory()) {
                    return (cb(null, {op: 'skip'}));
                }

                var opts = {
                    key: req.parentKey,
                    requestId: req.getId()
                };

                loadMetadata(req, opts, function (err, md, w) {
                    if (err) {
                        cb(err);
                    } else {
                        var p = req.parentKey;
                        if (req.isRootDirectory(p))
                            md.type = 'directory';
                        var obj = {
                            op: 'parent',
                            metadata: md,
                            etag: (w || {})._etag
                        };

                        if (w && w._node) {
                            obj.shard = w._node.pnode;
                        }

                        cb(null, obj);
                    }
                });
                return (undefined);
            }
        ]
    }, function (err, results) {
        if (err)
            return (next(err));

        results.successes.forEach(function (r) {
            switch (r.op) {
            case 'entry':
                req.metadata = r.metadata;
                req.metadata._etag = r.etag || null;
                req.metadata.headers =
                    req.metadata.headers || {};
                if (r.metadata.etag)
                    res.set('Etag', r.metadata.etag);
                if (r.metadata.mtime) {
                    var d = new Date(r.metadata.mtime);
                    res.set('Last-Modified', d);
                }

                req.entryShard = r.shard;  // useful for logging
                break;

            case 'parent':
                req.parentMetadata = r.metadata;

                req.parentShard = r.shard;  // useful for logging
                break;

            default:
                break;
            }
        });

        log.debug({
            metadata: req.metadata,
            parentMetadata: req.parentMetadata
        }, 'getMetadata: done');
        return (next());
    });
}


/*
 * Helper for getMetadata that looks up a key in Moray and fetches any roles
 * on the object.
 *
 * If the key doesn't exist, it passes a dummy metadata object to the callback
 * that can be used across various handlers.
 */
function loadMetadata(req, opts, callback) {
    req.moray.getMetadata(opts, function (err, md, wrap) {
        if (err) {
            if (VError.findCauseByName(err, 'ObjectNotFoundError') !== null) {
                md = {
                    type: (req.isRootDirectory() ?
                           'directory' :
                           null)
                };
            } else {
                return (callback(err, req));
            }
        }

        if (md.roles) {
            md.headers = md.headers || {};
            req.mahi.getName({
                uuids: md.roles
            }, function (err2, lookup) {
                if (err2) {
                    return (callback(err2));
                }
                if (md.roles && md.roles.length) {
                    md.headers['role-tag'] = md.roles.filter(function (uuid) {
                        return (lookup[uuid]);
                    }).map(function (uuid) {
                        return (lookup[uuid]);
                    }).join(', ');
                }
                return (callback(null, md, wrap));
            });
        } else {
            return (callback(null, md, wrap));
        }
    });
}


function addCustomHeaders(req, res) {
    var md = req.metadata.headers;
    var origin = req.headers.origin;

    Object.keys(md).forEach(function (k) {
        var add = false;
        var val = md[k];
        // See http://www.w3.org/TR/cors/#resource-requests
        if (origin && CORS_RES_HDRS.indexOf(k) !== -1) {
            if (k === 'access-control-allow-origin') {
                /* JSSTYLED */
                if (val.split(/\s*,\s*/).some(function (v) {
                    if (v === origin || v === '*') {
                        val = origin;
                        return (true);
                    }
                    return (false);
                })) {
                    add = true;
                } else {
                    CORS_RES_HDRS.forEach(function (h) {
                        res.removeHeader(h);
                    });
                }
            } else if (k === 'access-control-allow-methods') {
                /* JSSTYLED */
                if (val.split(/\s*,\s*/).some(function (v) {
                    return (v === req.method);
                })) {
                    add = true;
                } else {
                    CORS_RES_HDRS.forEach(function (h) {
                        res.removeHeader(h);
                    });
                }
            } else if (k === 'access-control-expose-headers') {
                add = true;
            }
        } else {
            add = true;
        }

        if (add)
            res.header(k, val);
    });
}

/*
 * The "opts" argument can contain the following fields:
 *   - checkParams: a boolean indicating whether to restrict the use of
 *     sensitive query parameters to operators. These sensitive parameters are:
 *     - sort === 'none'
 *     - skip_owner_check === 'true'
 */
function readdir(dir, req, opts) {
    if (opts === undefined) {
        opts = { checkParams: true };
    } else {
        assert.object(opts, 'opts');
        assert.bool(opts.checkParams, 'opts.checkParams');
    }

    /*
     * We check for the sensitive query parameters mentioned above, and, if
     * checkParams is specified, verify that the request came from an operator.
     *
     *
     *
     * ### When to use checkParams:
     *
     * The caller should pass checkParams as `true` anywhere readdir is being
     * called where the query parameters have been sent directly by the
     * client -- for example, when servicing a GET request on a directory.
     * The intent is to restrict the use of sensitive parameters to operators,
     * for reasons explained below.
     *
     * The caller can pass checkParams as `false` in any situation where both of
     * the following are true:
     *
     *  - The sensitive query parameters haven't come from the client -- in
     *    other words, the caller is using readdir "internally" as a helper
     *    function and is choosing to use the sensitive parameters for their
     *    functionality and/or performance benefit
     *
     *  - The directory listing (or sensitive information related to the
     *    directory listing) won't be sent back to the client, meaning it
     *    doesn't matter whether the client is an operator
     *
     * One representative scenario is when muskie checks if
     * a directory is empty before removing it, as explained below. Other such
     * scenarios are possible.
     *
     *
     *
     * ### What the sensitive parameters do:
     *
     * "sort === 'none'" is sensitive because it removes the guarantee of any
     * consistent total order of the results returned, or even that two
     * identical requests return the same results. This lack of ordering also
     * makes pagination unsafe, as there's no guarantee that each child will get
     * returned exactly once in a sequence of paginated requests. This behavior
     * is likely not what the client expects or wants, so we limit its use to
     * operators.
     *
     * "skip_owner_check === 'true'" is sensitive because it theoretically
     * allows users to see objects owned by other users that reside in the
     * directory being listed. We thus limit its use to operators as well.
     *
     * These two parameters constitute a short-term approach to a faster
     * directory listing, to be used in the garbage collection process that runs
     * in mako zones. In this use case, the client does not care about the order
     * of results returned (because it deletes all results it gets back before
     * asking for more) nor who owns the objects (because  it knows that only
     * poseidon has written to the directory in question).
     *
     * "sort === 'none'" is also used internally when checking if a directory
     * is empty, because we only care whether results exist, not what order they
     * are in.
     */
    var nosort = req.params.sort === 'none';
    var ownerCheck = !(req.params.skip_owner_check === 'true');

    var ee;
    if (opts.checkParams) {
        var isOperator = req.caller.account.isOperator;

        if (!isOperator) {
            var badParams = [];
            if (nosort) {
                badParams.push('sort=none');
            }
            if (!ownerCheck) {
                badParams.push('skip_owner_check=true');
            }
            if (badParams.length != 0) {
                ee = new EventEmitter();
                setImmediate(function () {
                    ee.emit('error',
                        new QueryParameterForbiddenError(badParams));
                });
                return (ee);
            }
        }
    }

    var l = parseInt(req.query.limit || 256, 10);
    if (l <= 0 || l > 1024) {
        ee = new EventEmitter();
        setImmediate(function () {
            ee.emit('error', new InvalidLimitError(l));
        });
        return (ee);
    }

    // We want the really low-level API here, as we want to go hit the place
    // where all the keys are, not where the dirent itself is.
    var client = req.moray;
    var filter = new morayFilter.AndFilter();

    if (ownerCheck) {
        var account = req.owner.account.uuid;
        filter.addFilter(new morayFilter.EqualityFilter({
            attribute: 'owner',
            value: account
        }));
    }
    filter.addFilter(new morayFilter.EqualityFilter({
        attribute: 'dirname',
        value: dir
    }));

    // The 'dir' above comes in as the path of the request.  The 'dir'
    // and 'obj' parameters are filters.
    var hasDir = (req.params.dir !== undefined ||
                  req.params.directory !== undefined);
    var hasObj = (req.params.obj !== undefined ||
                  req.params.object !== undefined);

    if ((hasDir || hasObj) && !(hasDir && hasObj)) {
        filter.addFilter(new morayFilter.EqualityFilter({
            attribute: 'type',
            value: (hasDir ? 'directory' : 'object')
        }));
    }

    var marker = req.query.marker;
    var reverse = req.params.sort_order === 'reverse';
    var tsort = req.params.sort === 'mtime';

    var log = req.log;
    var morayOpts = {
        limit: l,
        requestId: req.getId(),
        sort: {},
        hashkey: dir,
        no_count: true
    };

    if (tsort) {
        morayOpts.sort.attribute = '_mtime';
        if (reverse) {
            morayOpts.sort.order = 'ASC';
        } else {
            morayOpts.sort.order = 'DESC';
        }
    } else if (nosort) {
        delete morayOpts.sort;
    } else {
        // If we do not specify tsort or nosort, we sort by name.
        morayOpts.sort.attribute = 'name';
        if (reverse) {
            morayOpts.sort.order = 'DESC';
        } else {
            morayOpts.sort.order = 'ASC';
        }
    }

    /*
     * If a marker was provided with the request, add an appropriate filter so
     * that this page of results begins at the appropriate position in the full
     * result set.  If this is a request for unsorted results, it doesn't make
     * sense to use a marker as there is no consistent total order of the result
     * set.
     */
    if (marker && !nosort) {
        if (tsort) {
            var mtime = Date.parse(marker);
            if (Number.isFinite(mtime)) {
                marker = mtime.toString();
            } else {
                ee = new EventEmitter();
                setImmediate(function () {
                    ee.emit('error',
                            new InvalidParameterError('marker',
                                                      req.params.marker));
                });
                return (ee);
            }
        }

        var sortArgs = {
            attribute: morayOpts.sort.attribute,
            value: marker
        };
        if (morayOpts.sort.order === 'ASC') {
            filter.addFilter(new morayFilter.GreaterThanEqualsFilter(sortArgs));
        } else {
            filter.addFilter(new morayFilter.LessThanEqualsFilter(sortArgs));
        }
    }

    morayOpts.filter = filter.toString();

    log.debug({
        dir: dir,
        checkParams: opts.checkParams,
        morayOpts: morayOpts
    }, 'readdir: entered');
    var mreq = client.search(morayOpts);

    mreq.on('record', function (r) {
        if (r.key !== req.key) {
            var entry = {
                name: r.key.split('/').pop(),
                etag: r.value.etag,
                size: r.value.contentLength,
                type: r.value.type,
                contentType: r.value.contentType,
                contentDisposition: r.value.contentDisposition,
                contentMD5: r.value.contentMD5,
                mtime: new Date(r.value.mtime).toISOString()
            };

            if (entry.type === 'object')
                entry.durability = (r.value.sharks || []).length || 0;

            mreq.emit('entry', entry, r);
        }
    });

    return (mreq);
}



///--- Exports

module.exports = {

    ANONYMOUS_USER: ANONYMOUS_USER,

    STOR_PATH: STOR_PATH,

    PUBLIC_STOR_PATH: PUBLIC_STOR_PATH,

    REPORTS_STOR_PATH: REPORTS_STOR_PATH,

    PATH_LOGIN_RE: PATH_LOGIN_RE,

    UPLOADS_ROOT_PATH: UPLOADS_ROOT_PATH,

    MAX_HDRSIZE: MAX_HDRSIZE,

    METRIC_REQUEST_COUNTER: METRIC_REQUEST_COUNTER,

    METRIC_LATENCY_HISTOGRAM: METRIC_LATENCY_HISTOGRAM,

    METRIC_DURATION_HISTOGRAM: METRIC_DURATION_HISTOGRAM,

    METRIC_INBOUND_DATA_COUNTER: METRIC_INBOUND_DATA_COUNTER,

    METRIC_OUTBOUND_DATA_COUNTER: METRIC_OUTBOUND_DATA_COUNTER,

    METRIC_DELETED_DATA_COUNTER: METRIC_DELETED_DATA_COUNTER,

    storagePaths: function storagePaths(cfg) {
        var StoragePaths = {
            'public': {
                'name': 'Public',
                'regex': PUBLIC_STOR_PATH
            },
            'stor': {
                'name': 'Storage',
                'regex': STOR_PATH
            },
            'reports': {
                'name': 'Reports',
                'regex': REPORTS_STOR_PATH
            }
        };

        if (cfg.enableMPU) {
            StoragePaths.uploads = {
                'name': 'Uploads',
                'regex': UPLOADS_ROOT_PATH
            };
        }

        return (StoragePaths);
    },

    createMetadata: createMetadata,

    loadMetadata: loadMetadata,

    readdir: readdir,

    addCustomHeaders: addCustomHeaders,

    earlySetupHandler: function (opts) {
        assert.object(opts, 'options');

        function earlySetup(req, res, next) {
            res.once('header', function onHeader() {
                var now = Date.now();
                res.header('Date', new Date());
                res.header('Server', 'Manta/2');
                res.header('x-request-id', req.getId());

                var xrt = res.getHeader('x-response-time');
                if (xrt === undefined) {
                    var t = now - req.time();
                    res.header('x-response-time', t);
                }
                res.header('x-server-name', ZONENAME);
            });

            // Make req.isSecure() work as expected
            // We simply ensure that the request came in on the
            // standard port that is fronted by muppet, not the one
            // dedicated for cleartext connections
            var p = req.connection.address().port;
            req._secure = (p === opts.port);

            // This will only be null on the _first_ request, and in
            // that instance, we're guaranteed that HAProxy sent us
            // an X-Forwarded-For header
            if (!req.connection._xff) {
                // Clean up clientip if IPv6
                var xff = req.headers['x-forwarded-for'];
                if (xff) {
                    /* JSSTYLED */
                    xff = xff.split(/\s*,\s*/).pop() || '';
                    xff = xff.replace(/^(f|:)+/, '');
                    req.connection._xff = xff;
                } else {
                    req.connection._xff =
                        req.connection.remoteAddress;
                }
            }

            /*
             * This might seem over-gratuitous, but it's necessary.  Per the
             * node.js documentation, if the socket is destroyed, it is possible
             * for `remoteAddress' to be undefined later on when we attempt to
             * log the specifics around this request.  As an insurance policy
             * against that, save off the remoteAddress now.
             */
            req.remoteAddress = req.connection.remoteAddress;

            var ua = req.headers['user-agent'];
            if (ua && /^curl.+/.test(ua))
                res.set('Connection', 'close');

            next();
        }

        return (earlySetup);
    },

    authorizationParser: function (req, res, next) {
        req.authorization = {};

        if (!req.headers.authorization)
            return (next());

        var pieces = req.headers.authorization.split(' ', 2);
        if (!pieces || pieces.length !== 2) {
            var e = new restifyErrors.InvalidHeaderError(
                'Invalid Authorization header');
            return (next(e));
        }

        req.authorization.scheme = pieces[0];
        req.authorization.credentials = pieces[1];

        if (pieces[0].toLowerCase() === 'signature') {
            try {
                req.authorization.signature = httpSignature.parseRequest(req);
            } catch (e2) {
                var err = new restifyErrors.InvalidHeaderError(
                    'Invalid Signature ' + 'Authorization header: ' +
                    e2.message);
                return (next(err));
            }
        }

        next();
    },

    assertMetadataHandler: function () {
        return (assertMetadata);
    },

    enforceSSLHandler: function () {
        return (enforceSSL);
    },

    ensureEntryExistsHandler: function () {
        return (ensureEntryExists);
    },

    ensureNotDirectoryHandler: function () {
        return (ensureNotDirectory);
    },

    ensureNotRootHandler: function () {
        return (ensureNotRoot);
    },

    ensureParentHandler: function () {
        return (ensureParent);
    },

    getMetadataHandler: function () {
        return (getMetadata);
    },

    checkAccountSnaplinksMightExist: checkAccountSnaplinksMightExist,

    setupHandler: function (options, clients) {
        assert.object(options, 'options');
        assert.object(clients, 'clients');

        function setup(req, res, next) {
            // General request setup
            req.config = options;
            req.moray = clients.moray;

            /*
             * MANTA-331: while a trailing '/' is ok in HTTP, this messes with
             * the consistent hashing, so ensure there isn't one
             */
            var url = req.getUrl();
            /* JSSTYLED */
            url.pathname = req.getPath().replace(/\/*$/, '');

            req.log = (req.log || options.log).child({
                method: req.method,
                path: req.path(),
                req_id: req.getId()
            }, true);

            // Attach an artedi metric collector to each request object.
            req.collector = options.collector;

            req.sharks = [];
            req.sharkConfig = options.sharkConfig;
            req.sharkAgent = clients.sharkAgent;
            req.msk_defaults = {
                maxStreamingSize: options.storage.defaultMaxStreamingSizeMB *
                    1024 * 1024,
                mpuPrefixDirLen: options.multipartUpload.prefixDirLen
            };
            req.accountsSnaplinksDisabled = options.accountsSnaplinksDisabled;

            // Write request setup
            if (!req.isReadOnly()) {
                req.picker = clients.picker;
            }

            var _opts = {
                account: req.owner.account,
                path: req.path()
            };
            libmanta.normalizeMantaPath(_opts, function (err, p) {
                if (err) {
                    req.log.debug({
                        url: req.path(),
                        err: err
                    }, 'failed to normalize URL');
                    next(new InvalidPathError(req.path()));
                } else {
                    req.key = p;
                    if (!req.isRootDirectory()) {
                        req.parentKey =
                            path.dirname(req.key);
                    }

                    req.log.debug({
                        params: req.params,
                        path: req.path()
                    }, 'setup complete');
                    next();
                }
            });
        }

        return (setup);
    }
};
