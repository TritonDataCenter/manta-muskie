/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var EventEmitter = require('events').EventEmitter;
var http = require('http');
var once = require('once');
var os = require('os');
var path = require('path');
var util = require('util');
var httpSignature = require('http-signature');

var assert = require('assert-plus');
var libmanta = require('libmanta');
var morayFilter = require('moray-filter');
var vasync = require('vasync');
var restify = require('restify');
var VError = require('verror');

var CheckStream = require('./check_stream');
require('./errors');
var muskieUtils = require('./utils');
var Obj = ('./obj');
var sharkClient = require('./shark_client');
var utils = require('./utils');



///--- Globals

var clone = utils.shallowCopy;
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
var BUCKETS_ROOT_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\.@%]+)\/buckets\/?.*/;
/* JSSTYLED */
var BUCKETS_OBJECTS_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\.@%]+)\/buckets\/([a-zA-Z][a-zA-Z0-9_\.@%]+)\/objects\/.*/;
/* JSSTYLED */
var JOBS_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\.@%]+)\/jobs\/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})/;
/* JSSTYLED */
var JOBS_ROOT_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\.@%]+)\/jobs\/?.*/;
/* JSSTYLED */
var UPLOADS_ROOT_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\.@%]+)\/uploads\/?.*/;
/* JSSTYLED */
var JOBS_STOR_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\-\.@%]+)\/jobs\/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\/stor/;
var PUBLIC_STOR_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\-\.@%]+)\/public(\/(.*)|$)/;
var REPORTS_STOR_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\-\.@%]+)\/reports(\/(.*)|$)/;
var STOR_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\-\.@%]+)\/stor(\/(.*)|$)/;
/* JSSTYLED */
var MEDUSA_ROOT_PATH = /^\/([a-zA-Z][a-zA-Z0-9_\.@%]+)\/medusa\/?.*/;

// Thanks for being a PITA, javascriptlint (it doesn't like /../ form in [])
var ROOT_REGEXPS = [
    new RegExp('^\\/[a-zA-Z0-9_\\-\\.@%]+$'), // /:login
    new RegExp('^\\/[a-zA-Z0-9_\\-\\.@%]+\\/public\\/?$'), // public
    new RegExp('^\\/[a-zA-Z0-9_\\-\\.@%]+\\/stor\\/?$'), // storage
    new RegExp('^\\/[a-zA-Z0-9_\\-\\.@%]+\\/jobs\\/?$'), // jobs (list)
    new RegExp('^\\/[a-zA-Z0-9_\\-\\.@%]+\\/uploads\\/?$'), // uploads (list)
    new RegExp('^\\/[a-zA-Z0-9_\\-\\.@%]+\\/buckets\\/?$'), // buckets (list)

    // jobs storage
    new RegExp('^\\/[a-zA-Z0-9_\\-\\.@%]+\\/jobs\\/[\\w-]+\\/stor\\/?$'),
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


HttpRequest.encodeBucketObject = function encodeBucketObject() {
    var self = this;

    var splitPath = self.path().split('/');
    /* This slice is :account/buckets/:bucketname/objects/ */
    var baseBucketObjectPath = splitPath.slice(0, 5).join('/');

    var bucketObject = self.path().split('/objects/').pop();
    var encodedBucketObject = encodeURIComponent(bucketObject);
    var pathParts = [baseBucketObjectPath, encodedBucketObject];

    self._path = pathParts.join('/');
    return (self._path);
};


HttpRequest.isConditional = function isConditional() {
    return (this.headers['if-match'] !== undefined ||
            this.headers['if-none-match'] !== undefined);
};


HttpRequest.isMarlinRequest = function isMarlinRequest() {
    return (JOBS_ROOT_PATH.test(this.path()));
};

HttpRequest.isMedusaRequest = function isMedusaRequest() {
    return (MEDUSA_ROOT_PATH.test(this.path()));
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


HttpRequest.isBucketRoot = function isBucketRoot() {
    function _test(p) {
        return (BUCKETS_ROOT_PATH.test(p));
    }

    return (_test(this.path()));
};

HttpRequest.isBucketObject = function isBucketObject() {
    function _test(p) {
        return (BUCKETS_OBJECTS_PATH.test(p));
    }

    return (_test(this.path()));
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
    return (JOBS_PATH.test(p) || REPORTS_STOR_PATH.test(p));
};



///--- API

function createMetadata(req, type, cb) {
    var prev = req.metadata || {};
    /*
     * Override the UpdateMetadata type, as this flows in via PUT Object path.
     */
    if (prev.type === 'directory')
        type = 'directory';

    /*
     * This allows bucket objects to be created with slashes in their names that
     * are interpreted as part of the object name, rather than denoting a parent
     * directory the object resides in.
     */
    var mdDirname;
    if (type !== 'bucketobject') {
        mdDirname = path.dirname(req.key);
    } else {
        /* This cuts the path down to /:account/buckets/:bucketname/objects. */
        mdDirname = req.key.split('/').slice(0, 5).join('/');
    }
    assert.string(mdDirname, 'mdDirname');

    var names;
    var md = {
        dirname: mdDirname,
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
    case 'bucket':
    case 'directory':
        break;

    case 'link':
        md.link = req.link.metadata;
        break;

    case 'bucketobject':
    case 'object':
        muskieUtils.validateContentDisposition(
            req.headers, function cdcb(err, _h) {
                if (err) {
                    req.log.debug('malformed content-disposition: %s', err.msg);
                    cb(new restify.errors.BadRequestError());
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
 * that a particular account uuid has snaplinks enabled. Today, it is
 * called in the `ensureSnaplinksEnabled` method defined in this module
 * and in the putlinkHandler chain to verify that the source object (the
 * object being linked to) is not owned by an account for which snaplinks
 * are enabled.
 */
function checkAccountSnaplinksEnabled(req, uuid, next) {
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


function ensureSnaplinksEnabled(req, res, next) {
    checkAccountSnaplinksEnabled(req, req.caller.account.uuid,
        function (enabled) {
        if (!enabled) {
            next(new SnaplinksDisabledError('snaplinks have been disabled ' +
                'for this account'));
            return;
        }
    });
    next();
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

    if (req.method === 'DELETE' && !JOBS_PATH.test(req.path())) {
        next(new RootDirectoryError(req.method, req.path()));
        return;
    }

    next();
}


function ensureBucket(req, res, next) {
    req.log.debug({
        path: req.path
    }, 'ensureBucket: entered');

    if (req.isBucketRoot()) {
        req.log.debug('ensureBucket: done');
        next();
    } else {
        next(new ParentNotBucketRootError(req));
    }
}


function ensureBucketObject(req, res, next) {
    req.log.debug({
        path: req.path
    }, 'ensureBucketObject: entered');

    if (req.isBucketObject()) {
        req.log.debug('ensureBucketObject: done');
        next();
    } else {
        next(new ParentNotBucketError(req));
    }
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
    if (req.isBucketRoot()) {
        return (next());
    }

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

    var l = parseInt(req.params.limit || 256, 10);
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

    var marker = req.params.marker;
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



function findSharks(req, res, next) {
    if (req._zero || req.query.metadata) {
        next();
        return;
    }

    var log = req.log;
    var opts = {
        replicas: req._copies,
        requestId: req.getId(),
        size: req._size,
        isOperator: req.caller.account.isOperator
    };

    log.debug(opts, 'findSharks: entered');

    opts.log = req.log;
    req.picker.choose(opts, function (err, sharks) {
        if (err) {
            next(err);
        } else {
            req._sharks = sharks;
            log.debug({
                sharks: req._sharks
            }, 'findSharks: done');
            next();
        }
    });
}


/*
 * This handler attempts to connect to one of the pre-selected, cross-DC sharks.
 * If a connection to any shark in the set fails, we try a different set of
 * sharks.
 */
function startSharkStreams(req, res, next) {
    if (req._zero || req.query.metadata) {
        next();
        return;
    }

    assert.ok(req._sharks);

    var log = req.log;
    log.debug({
        objectId: req.objectId,
        sharks: req._sharks
    }, 'startSharkStreams: entered');

    var ndx = 0;
    var opts = {
        contentType: req.getContentType(),
        contentLength: req.isChunked() ? undefined : req._size,
        contentMd5: req.headers['content-md5'],
        objectId: req.objectId,
        owner: req.owner.account.uuid,
        requestId: req.getId(),
        sharkConfig: req.sharkConfig,
        sharkAgent: req.sharkAgent
    };

    req.sharksContacted = [];

    (function attempt(inputs) {
        vasync.forEachParallel({
            func: function shark_connect(shark, cb) {
                var _opts = clone(opts);
                _opts.log = req.log;
                _opts.shark = shark;

                var sharkInfo = createSharkInfo(req, shark.manta_storage_id);
                sharkConnect(_opts, sharkInfo, cb);
            },
            inputs: inputs
        }, function (err, results) {
            req.sharks = results.successes || [];
            if (err || req.sharks.length < req._copies) {
                log.debug({
                    err: err,
                    sharks: inputs
                }, 'startSharkStreams: failed');

                req.abandonSharks();
                if (ndx < req._sharks.length) {
                    attempt(req._sharks[ndx++]);
                } else {
                    next(new SharksExhaustedError(res));
                }
                return;
            }
            if (log.debug()) {
                req.sharks.forEach(function (s) {
                    s.headers = s._headers;
                    log.debug({
                        client_req: s
                    }, 'mako: stream started');
                });

                log.debug({
                    objectId: req.objectId,
                    sharks: inputs
                }, 'startSharkStreams: done');
            }
            next();
        });
    })(req._sharks[ndx++]);
}


/*
 * Here we stream the data from the object to each connected shark, using a
 * check stream to compute the md5 sum of the data as it passes through muskie
 * to mako.
 *
 * This handler is blocking.
 */
function sharkStreams(req, res, next) {
    if (req._zero || req.query.metadata) {
        next();
        return;
    }

    /*
     * While in the process of streaming the object out to multiple sharks, if a
     * failure is experienced on one stream, we will essentially treat it as an
     * overall failure and abandon the process of streaming this object to all
     * sharks involved.  Note that `next_err()' is wrapped in the `once()'
     * method because we need only respond to a failure event once.
     */
    var next_err = once(function _next_err(err) {
        req.log.debug({
            err: err
        }, 'abandoning request');

        /* Record the number of bytes that we transferred. */
        req._size = check.bytes;

        req.removeListener('end', onEnd);
        req.removeListener('error', next_err);

        req.abandonSharks();
        req.unpipe(check);
        check.abandon();

        next(err);
    });

    var barrier = vasync.barrier();
    var check = new CheckStream({
        algorithm: 'md5',
        maxBytes: req._size,
        timeout: Obj.DATA_TIMEOUT,
        counter: req.collector.getCollector(METRIC_INBOUND_DATA_COUNTER)
    });
    var log = req.log;

    req.domain.add(check);

    barrier.once('drain', function onCompleteStreams() {
        req._timeToLastByte = Date.now();

        req.connection.removeListener('error', abandonUpload);
        req.removeListener('error', next_err);

        if (req.sharks.some(function (s) {
            return (s.md5 !== check.digest('base64'));
        })) {
            var _md5s = req.sharks.map(function (s) {
                return (s.md5);
            });
            log.error({
                clientMd5: req.headers['content-md5'],
                muskieMd5: check.digest('base64'),
                makoMd5: _md5s
            }, 'mako didnt recieve what muskie sent');
            var m = new VError('muskie md5 %s and mako md5 ' +
                            '%s don\'t match', check.digest('base64'),
                            _md5s.join());
            next_err(new InternalError(m));
        } else {
            log.debug('sharkStreams: done');
            next();
        }
    });

    log.debug('streamToSharks: streaming data');

    function abandonUpload() {
        next_err(new UploadAbandonedError());
    }

    req.connection.once('error', abandonUpload);

    req.once('error', next_err);

    barrier.start('client');
    req.pipe(check);
    req.sharks.forEach(function (s) {
        barrier.start(s._shark.manta_storage_id);
        req.pipe(s);
        s.once('response', function onSharkResult(sres) {
            log.debug({
                mako: s._shark.manta_storage_id,
                client_res: sres
            }, 'mako: response received');

            var sharkInfo = getSharkInfo(req, s._shark.manta_storage_id);
            sharkInfo.timeTotal = Date.now() - sharkInfo._startTime;
            sharkInfo.result = 'fail'; // most cases below here are failures

            s.md5 = sres.headers['x-joyent-computed-content-md5'] ||
                req._contentMD5;
            if (sres.statusCode === 469) {
                next_err(new ChecksumError(s.md5, req.headers['content-md5']));
            } else if (sres.statusCode === 400 && req.headers['content-md5']) {
                next_err(new restify.BadRequestError('Content-MD5 invalid'));
            } else if (sres.statusCode > 400) {
                var body = '';
                sres.setEncoding('utf8');
                sres.on('data', function (chunk) {
                    body += chunk;
                });
                sres.once('end', function () {
                    log.debug({
                        mako: s._shark.manta_storage_id,
                        client_res: sres,
                        body: body
                    }, 'mako: response error');
                    var m = new VError('mako response error, storage id (%s)',
                        s._shark.manta_storage_id);
                    next_err(new InternalError(m));
                });
                sres.once('error', function (err) {
                    next_err(new InternalError(err));
                });
            } else {
                sharkInfo.result = 'ok';
                barrier.done(s._shark.manta_storage_id);
            }
            /*
             * Even though PUT requests that are successful normally result
             * in an empty resonse body from nginx, we still need to make sure
             * we let the response stream emit 'end'. Otherwise this will jam
             * up keep-alive agent connections (the node http.js needs that
             * 'end' even to happen before relinquishing the socket).
             *
             * Easiest thing to do is just call resume() which should make the
             * stream run out and emit 'end'.
             */
            sres.resume();
        });
    });

    check.once('timeout', function () {
        res.header('connection', 'close');
        next_err(new UploadTimeoutError());
    });

    check.once('length_exceeded', function (sz) {
        next_err(new MaxSizeExceededError(sz));
    });

    check.once('error', next_err);

    function onEnd() {
        // We replace the actual size, in case it was streaming, and
        // the content-md5 we actually calculated on the wire
        req._contentMD5 = check.digest('base64');
        req._size = check.bytes;
        barrier.done('client');
    }

    req.once('end', onEnd);

    barrier.start('check_stream');
    check.once('done', function () {
        barrier.done('check_stream');
    });

    if (req.header('expect') === '100-continue') {
        res.writeContinue();
        log.info({
            remoteAddress: req.connection._xff,
            remotePort: req.connection.remotePort,
            req_id: req.id,
            latency: (Date.now() - req._time),
            'audit_100': true
        }, '100-continue sent');
    }

    req._timeAtFirstByte = Date.now();
}

// Here we pick a shark to talk to, and the first one that responds we
// just stream from. After that point any error is an internal error.
function streamFromSharks(req, res, next) {
    if (req.metadata.type !== 'object' &&
        req.metadata.type !== 'bucketobject') {
            next();
            return;
    }

    var connected = false;
    var log = req.log;
    var md = req.metadata;
    var opts = {
        owner: req.owner.account.uuid,
        creator: md.creator,
        objectId: md.objectId,
        requestId: req.getId()
    };
    var queue;
    var savedErr = false;

    if (req.headers.range)
        opts.range = req.headers.range;

    log.debug('streamFromSharks: entered');

    addCustomHeaders(req, res);

    if (md.contentLength === 0 || req.method === 'HEAD') {
        log.debug('streamFromSharks: HEAD || zero-byte object');
        res.header('Durability-Level', req.metadata.sharks.length);
        res.header('Content-Disposition', req.metadata.contentDisposition);
        res.header('Content-Length', md.contentLength);
        res.header('Content-MD5', md.contentMD5);
        res.header('Content-Type', md.contentType);
        res.send(200);
        next();
        return;
    }

    req.sharksContacted = [];

    function respond(shark, sharkReq, sharkInfo) {
        log.debug('streamFromSharks: streaming data');
        // Response headers
        var sh = shark.headers;
        if (req.headers['range'] !== undefined) {
            res.header('Content-Type', sh['content-type']);
            res.header('Content-Range', sh['content-range']);
        } else {
            res.header('Accept-Ranges', 'bytes');
            res.header('Content-Type', md.contentType);
            res.header('Content-MD5', md.contentMD5);
        }

        res.header('Content-Disposition', req.metadata.contentDisposition);
        res.header('Content-Length', sh['content-length']);
        res.header('Durability-Level', req.metadata.sharks.length);

        req._size = sh['content-length'];

        // Response body
        req._totalBytes = 0;
        var check = new CheckStream({
            maxBytes: parseInt(sh['content-length'], 10) + 1024,
            timeout: Obj.DATA_TIMEOUT,
            counter: req.collector.getCollector(
                METRIC_OUTBOUND_DATA_COUNTER)
        });
        sharkInfo.timeToFirstByte = check.start - sharkInfo._startTime;
        check.once('done', function onCheckDone() {
            req.connection.removeListener('error', onConnectionClose);

            if (check.digest('base64') !== md.contentMD5 &&
                !req.headers.range) {
                // We can't set error now as the header has already gone out
                // MANTA-1821, just stop logging this for now XXX
                log.warn({
                    expectedMD5: md.contentMD5,
                    returnedMD5: check.digest('base64'),
                    expectedBytes: parseInt(sh['content-length'], 10),
                    computedBytes: check.bytes,
                    url: req.url
                }, 'GetObject: partial object returned');
                res.statusCode = 597;
            }

            log.debug('streamFromSharks: done');
            req._timeAtFirstByte = check.start;
            req._timeToLastByte = Date.now();
            req._totalBytes = check.bytes;

            sharkInfo.timeTotal = req._timeToLastByte - sharkInfo._startTime;

            next();
        });
        shark.once('error', next);

        function onConnectionClose(err) {
            /*
             * It's possible to invoke this function through multiple paths, as
             * when a socket emits 'error' and the request emits 'close' during
             * this phase.  But we only want to handle this once.
             */
            if (req._muskie_handle_close) {
                return;
            }

            req._muskie_handle_close = true;
            req._probes.client_close.fire(function onFire() {
                var _obj = {
                    id: req._id,
                    method: req.method,
                    headers: req.headers,
                    url: req.url,
                    bytes_sent: check.bytes,
                    bytes_expected: parseInt(sh['content-length'], 10)
                };
                return ([_obj]);
            });

            req.log.warn(err, 'handling closed client connection');
            check.removeAllListeners('done');
            shark.unpipe(check);
            shark.unpipe(res);
            sharkReq.abort();
            req._timeAtFirstByte = check.start;
            req._timeToLastByte = Date.now();
            req._totalBytes = check.bytes;
            res.statusCode = 499;
            next(false);
        }

        /*
         * It's possible that the client has already closed its connection at
         * this point, in which case we need to abort the request here in order
         * to avoid coming to rest in a broken state.  You might think we'd
         * notice this problem when we pipe the mako response to the client's
         * response and attempt to write to a destroyed Socket, but instead Node
         * drops such writes without emitting an error.  (It appears to assume
         * that the caller will be listening for 'close'.)
         */
        if (req._muskie_client_closed) {
            setImmediate(onConnectionClose,
                new Error('connection closed before streamFromSharks'));
        } else {
            req.connection.once('error', onConnectionClose);
            req.once('close', function () {
                onConnectionClose(new Error(
                    'connection closed during streamFromSharks'));
            });
        }

        res.writeHead(shark.statusCode);
        shark.pipe(check);
        shark.pipe(res);
    }

    queue = libmanta.createQueue({
        limit: 1,
        worker: function start(s, cb) {
            if (connected) {
                cb();
            } else {
                var sharkInfo = createSharkInfo(req, s.hostname);

                s.get(opts, function (err, cReq, cRes) {
                    if (err) {
                        sharkInfo.result = 'fail';
                        sharkInfo.timeTotal = Date.now() - sharkInfo._startTime;
                        log.warn({
                            err: err,
                            shark: s.toString()
                        }, 'mako: connection failed');
                        savedErr = err;
                        cb();
                    } else {
                        sharkInfo.result = 'ok';
                        connected = true;
                        respond(cRes, cReq, sharkInfo);
                        cb();
                    }
                });
            }
        }
    });

    queue.once('end', function () {
        if (!connected) {
            // Honor Nginx handling Range GET requests
            if (savedErr && savedErr._result) {
                var rh = savedErr._result.headers;
                if (req.headers['range'] !== undefined && rh['content-range']) {
                    res.setHeader('content-range', rh['content-range']);
                    next(new restify.RequestedRangeNotSatisfiableError());
                    return;
                }
            }
            next(savedErr || new InternalError());
        }
    });

    var shuffledSharks = utils.shuffle(req.metadata.sharks);

    shuffledSharks.forEach(function (s) {
        queue.push(sharkClient.getClient({
            connectTimeout: req.sharkConfig.connectTimeout,
            log: req.log,
            retry: req.sharkConfig.retry,
            shark: s,
            agent: req.sharkAgent
        }));
    });

    queue.close();
}

// Simple wrapper around sharkClient.getClient + put
//
// opts:
//   {
//      contentType: req.getContentType(),   // content-type from the request
//      contentLength: req.isChunked() ? undefined : req._size,
//      log: $bunyan,
//      shark: $shark,  // a specific shark from $picker.choose()
//      objectId: req.objectId,    // proposed objectId
//      owner: req.owner.account.uuid,   // /:login/stor/... (uuid for $login)
//      sharkConfig: {  // from config.json
//        connectTimeout: 4000,
//        retry: {
//          retries: 2
//        }
//      },
//      requestId: req.getId()   // current request_id
//   }
//
// sharkInfo: object used for logging information about the shark
//
function sharkConnect(opts, sharkInfo, cb) {
    var client = sharkClient.getClient({
        connectTimeout: opts.sharkConfig.connectTimeout,
        log: opts.log,
        retry: opts.sharkConfig.retry,
        shark: opts.shark,
        agent: opts.sharkAgent
    });
    assert.ok(client, 'sharkClient returned null');

    client.put(opts, function (err, req) {
        if (err) {
            cb(err);
        } else {
            req._shark = opts.shark;
            opts.log.debug({
                client_req: req
            }, 'SharkClient: put started');
            sharkInfo.timeToFirstByte = Date.now() - sharkInfo._startTime;
            cb(null, req);
        }
    });
}

// Creates a 'sharkInfo' object, used for logging purposes,
// and saves it on the input request object to log later.
//
// Input:
//      req: the request object to save this shark on
//      hostname: the name of the shark (e.g., '1.stor.emy-13.joyent.us')
// Output:
//      a sharkInfo object
function createSharkInfo(req, hostname) {
    var sharkInfo = {
        shark: hostname,
        result: null, // 'ok' or 'fail'
        // time until streaming object to or from the shark begins
        timeToFirstByte: null,
        timeTotal: null, // total request time

        // private: time request begins (used to calculate other time values)
        _startTime: Date.now()
    };

    req.sharksContacted.push(sharkInfo);
    return (sharkInfo);
}

// Given a request object and shark name, returns the matching sharkInfo object.
// This is only meant to be used if we are certain the shark is in this request,
// and will cause an assertion failure otherwise.
function getSharkInfo(req, hostname) {
    var sharks = req.sharksContacted.filter(function (sharkInfo) {
        return (sharkInfo.shark === hostname);
    });

    assert.equal(sharks.length, 1, 'There should only be one sharkInfo ' +
        'with hostname "' + hostname + '"');

    return (sharks[0]);
}

///--- Exports

module.exports = {

    ANONYMOUS_USER: ANONYMOUS_USER,

    CORS_RES_HDRS: CORS_RES_HDRS,

    JOBS_PATH: JOBS_PATH,

    STOR_PATH: STOR_PATH,

    JOBS_STOR_PATH: JOBS_STOR_PATH,

    PUBLIC_STOR_PATH: PUBLIC_STOR_PATH,

    REPORTS_STOR_PATH: REPORTS_STOR_PATH,

    PATH_LOGIN_RE: PATH_LOGIN_RE,

    UPLOADS_ROOT_PATH: UPLOADS_ROOT_PATH,

    BUCKETS_ROOT_PATH: BUCKETS_ROOT_PATH,

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
            'jobs': {
                'name': 'Jobs',
                'regex': JOBS_ROOT_PATH
            },
            'reports': {
                'name': 'Reports',
                'regex': REPORTS_STOR_PATH
            },
            'buckets': {
                'name': 'Buckets',
                'buckets': BUCKETS_ROOT_PATH
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
                res.header('Server', 'Manta');
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
            var e = new restify.InvalidHeaderError(
                'Invalid Authorization header');
            return (next(e));
        }

        req.authorization.scheme = pieces[0];
        req.authorization.credentials = pieces[1];

        if (pieces[0].toLowerCase() === 'signature') {
            try {
                req.authorization.signature = httpSignature.parseRequest(req);
            } catch (e2) {
                var err = new restify.InvalidHeaderError('Invalid Signature ' +
                    'Authorization header: ' + e2.message);
                throw (err);
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

    ensureSnaplinksEnabledHandler: function () {
        return (ensureSnaplinksEnabled);
    },

    ensureNotDirectoryHandler: function () {
        return (ensureNotDirectory);
    },

    ensureNotRootHandler: function () {
        return (ensureNotRoot);
    },

    ensureBucketRootHandler: function () {
        return (ensureBucket);
    },

    ensureBucketObjectHandler: function () {
        return (ensureBucketObject);
    },

    ensureParentHandler: function () {
        return (ensureParent);
    },

    getMetadataHandler: function () {
        return (getMetadata);
    },

    checkAccountSnaplinksEnabled: checkAccountSnaplinksEnabled,

    setupHandler: function (options, clients) {
        assert.object(options, 'options');
        assert.object(options.jobCache, 'options.jobCache');
        assert.object(options.log, 'options.log');
        assert.object(options.collector, 'options.collector');
        assert.object(clients.keyapi, 'clients.keyapi');
        assert.object(clients.mahi, 'clients.mahi');
        assert.object(clients.marlin, 'clients.marlin');
        assert.object(clients.picker, 'clients.picker');
        assert.optionalObject(options.moray, 'options.moray');
        assert.object(options.boray, 'options.boray');
        assert.object(clients.medusa, 'clients.medusa');
        assert.object(options.sharkConfig, 'options.sharkConfig');
        assert.object(options.storage, 'options.storage');
        assert.number(options.storage.defaultMaxStreamingSizeMB,
            'options.storage.defaultMaxStreamingSizeMB');
        assert.object(options.multipartUpload, 'options.multipartUpload');
        assert.number(options.multipartUpload.prefixDirLen,
            'options.multipartUpload.prefixDirLen');
        assert.arrayOfObject(options.accountsSnaplinksDisabled,
            'options.accountsSnaplinksDisabled');

        function setup(req, res, next) {
            req.config = options;
            req.moray = clients.moray;
            req.boray = clients.boray;

            // MANTA-331: while a trailing '/' is ok in HTTP,
            // this messes with the consistent hashing, so
            // ensure there isn't one
            /* JSSTYLED */
            req._path = req._path.replace(/\/*$/, '');

            req.jobCache = options.jobCache;

            req.log = (req.log || options.log).child({
                method: req.method,
                path: req.path(),
                req_id: req.getId()
            }, true);

            // Attach an artedi metric collector to each request object.
            req.collector = options.collector;

            req.marlin = clients.marlin;
            req.picker = clients.picker;
            req.sharks = [];
            req.sharkConfig = options.sharkConfig;
            req.sharkAgent = clients.sharkAgent;
            req.medusa = clients.medusa;
            req.msk_defaults = {
                maxStreamingSize: options.storage.defaultMaxStreamingSizeMB *
                    1024 * 1024,
                mpuPrefixDirLen: options.multipartUpload.prefixDirLen
            };
            req.accountsSnaplinksDisabled = options.accountsSnaplinksDisabled;

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
    },

    findSharks: findSharks,
    startSharkStreams: startSharkStreams,
    sharkStreams: sharkStreams,
    streamFromSharks: streamFromSharks

};
