/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var artedi = require('artedi');
var crypto = require('crypto');
var fs = require('fs');
var path = require('path');
var url = require('url');
var verror = require('verror');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var mime = require('mime');
var restify = require('restify');

var audit = require('./audit');
var auth = require('./auth');
var common = require('./common');
var dir = require('./dir');
var link = require('./link');
var obj = require('./obj');
var other = require('./other');
var picker = require('./picker');
var uploads = require('./uploads');
var throttle = require('./throttle');

var muskieUtils = require('./utils');

// injects into the global namespace
require('./errors');



///--- Globals

/*
 * from https://www.w3.org/Protocols/rfc1341/4_Content-Type.html
 * match 'type/subtype' where subtypes can be +/- delimited
 */
var VALID_CONTENT_TYPE_RE = /.+\/.+/;
/* END JSSTYLED */

///--- Helpers

// Always force JSON
function formatJSON(req, res, body) {
    if (body instanceof Error) {
        body = translateError(body, req);
        res.statusCode = body.statusCode || 500;
        if (res.statusCode >= 500)
            req.log.warn(body, 'request failed: internal error');

        if (body.headers !== undefined) {
            for (var h in body.headers) {
                res.setHeader(h, body.headers[h]);
            }
        }

        if (body.body) {
            body = body.body;
        } else {
            body = {
                message: body.message
            };
        }

    } else if (Buffer.isBuffer(body)) {
        body = body.toString('base64');
    }

    var data = JSON.stringify(body);
    var md5 = crypto.createHash('md5').update(data).digest('base64');

    res.setHeader('Content-Length', Buffer.byteLength(data));
    res.setHeader('Content-MD5', md5);
    res.setHeader('Content-Type', 'application/json');

    return (data);
}



///--- API

/**
 * Wrapper over restify's createServer to make testing and
 * configuration handling easier.
 *
 * @param {object} options            - options object.
 * @param {object} options.log        - bunyan logger.
 * @param {object} options.collector  - artedi metric collector.
 * @param {object} clients            - client connection object.
 * @param {string} name               - the application name.
 * @throws {TypeError} on bad input.
 */
function createServer(options, clients, name) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.collector, 'options.collector');
    assert.object(options.throttle, 'options.throttle');
    assert.object(clients, 'clients');
    assert.string(name, 'name');

    options.formatters = {
        'application/json': formatJSON,
        'text/plain': formatJSON,
        'application/octet-stream': formatJSON,
        'application/x-json-stream': formatJSON,
        '*/*': formatJSON
    };
    options.noWriteContinue = true;
    options.handleUpgrades = true;

    var log = options.log.child({
        component: 'HttpServer'
    }, true);
    var server = restify.createServer(options);

    /* Initialize metric collectors for use in handlers and audit logger. */
    // A counter to track the number of HTTP requests serviced.
    options.collector.counter({
        name: common.METRIC_REQUEST_COUNTER,
        help: 'count of Muskie requests completed'
    });
    /*
     * A mostly log-linear histogram to track the time to first byte.
     * Track values between 5 and 60000 ms (5ms to 1 minute).
     */
    options.collector.histogram({
        name: common.METRIC_LATENCY_HISTOGRAM,
        help: 'time-to-first-byte of Muskie requests',
        // These were generated with artedi.logLinearBuckets(10, 1, 3, 10); and
        // then some manual tweaking. See MANTA-4388 for details.
        buckets: [
            5,
            10,
            20,
            30,
            40,
            50,
            60,
            70,
            80,
            90,
            100,
            200,
            300,
            400,
            500,
            600,
            700,
            800,
            900,
            1000,
            2000,
            4000,
            6000,
            8000,
            10000,
            30000,
            60000
        ]
    });
    // A pair of counters to track inbound and outbound throughput.
    options.collector.counter({
        name: common.METRIC_INBOUND_DATA_COUNTER,
        help: 'count of object bytes streamed from client to storage'
    });
    options.collector.counter({
        name: common.METRIC_OUTBOUND_DATA_COUNTER,
        help: 'count of object bytes streamed from storage to client'
    });
    options.collector.counter({
        name: common.METRIC_DELETED_DATA_COUNTER,
        help: 'count of deleted object bytes'
    });
    options.collector.counter({
        name: common.METRIC_DELETED_OBJECT_COUNTER,
        help: 'count of deleted objects'
    });
    options.collector.counter({
        name: common.METRIC_DELETED_DIRECTORY_COUNTER,
        help: 'count of deleted directories'
    });

    var _timeout = parseInt((process.env.SOCKET_TIMEOUT || 120), 10) * 1000;
    server.server.setTimeout(_timeout, function onTimeout(socket) {
        var l = (((socket._httpMessage || {}).req || {}).log || log);
        var req = socket.parser && socket.parser.incoming;
        var res = socket._httpMessage;

        if (req && req.complete && res) {
            l.warn('socket timeout: destroying connection');
            options.dtrace_probes.socket_timeout.fire(function onFire() {
                var dobj = req ? {
                    method: req.method,
                    url: req.url,
                    headers: req.headers,
                    id: req._id
                } : {};
                return ([dobj]);
            });
            socket.destroy();
        }
    });

    server.pre(function watchClose(req, res, next) {
        /*
         * In some cases, we proactively check for closed client connections.
         * Add a listener early on that just records this fact.
         */
        req.on('close', function () {
            req.log.warn('client closed connection');
            req._muskie_client_closed = true;
        });

        next();
    });
    server.pre(function stashPath(req, res, next) {
        req._probes = options.dtrace_probes;
        req.config = options;
        req.pathPreSanitize = url.parse(req.url).pathname;
        next();
    });
    server.pre(restify.pre.sanitizePath());
    server.pre(function cleanupContentType(req, res, next) {
        var ct = req.headers['content-type'];
        /*
         * content-type must have a type, '/' and sub-type
         */
        if (ct && !VALID_CONTENT_TYPE_RE.test(ct)) {
            req.log.debug('receieved a malformed content-type: %s', ct);
            req.headers['content-type'] = mime.lookup(ct);
        }

        next();
    });

    // set up random stuff
    other.mount(server, clients);

    server.use(function _traceTTFB(req, res, next) {
        //
        // When it sends the header, restify's response object emits `header`.
        // See the `Response.prototype.writeHead` function.
        //
        // We use that here as our best proxy for time to first byte. Since the
        // header is the first part of our response. Some methods (specifically
        // streamFromSharks and sharkStreams) will override this to set their
        // own idea of when the first byte was (which might be before we send
        // anything to the client). The `audit.auditLogger` in the `after`
        // handler will use the final value when writing out metrics.
        //
        res.once('header', function _onHeader() {
            if (!req._timeAtFirstByte) {
                req._timeAtFirstByte = Date.now();
            }
        });

        next();
    });

    server.use(common.earlySetupHandler(options));
    server.use(restify.plugins.dateParser(options.maxRequestAge || 300));
    server.use(restify.plugins.gzipResponse());
    server.use(restify.plugins.queryParser());
    server.use(common.authorizationParser);
    server.use(auth.checkIfPresigned);
    server.use(common.enforceSSLHandler(options));

    server.use(function ensureDependencies(req, res, next) {
        var ok = true;
        var errors = [];
        var error;

        if (!clients.moray) {
            error = 'index moray unavailable';
            errors.push(new Error(error));
            req.log.error(error);
            ok = false;
        }

        if (!clients.mahi) {
            error = 'mahi unavailable';
            errors.push(new Error(error));
            req.log.error(error);
            ok = false;
        }

        if (!clients.picker && !clients.storinfo && !req.isReadOnly()) {
            error = 'picker/storinfo unavailable';
            errors.push(new Error(error));
            req.log.error(error);
            ok = false;
        }

        if (!ok) {
            next(new ServiceUnavailableError(req,
                        new verror.MultiError(errors)));
        } else {
            next();
        }
    });

    if (options.throttle.enabled) {
        options.throttle.log = options.log;
        var throttleHandle = throttle.createThrottle(options.throttle);
        server.use(throttle.throttleHandler(throttleHandle));
    }
    server.use(auth.authenticationHandler({
        log: log,
        mahi: clients.mahi,
        keyapi: clients.keyapi
    }));

    server.use(auth.gatherContext);

    // Add various fields to the 'req' object before the handlers get called.
    server.use(common.setupHandler(options, clients));


    // Multipart Upload API
    if (options.enableMPU) {
        addMultipartUploadRoutes(server);
    }

    server.use(common.getMetadataHandler());
    server.use(auth.storageContext);
    server.use(auth.authorizationHandler());

    // Tokens

    server.post({
        path: '/:account/tokens',
        name: 'CreateToken'
    }, auth.postAuthTokenHandler());

    // Data plane

    if (options.enableMPU) {
        addMultipartUploadDataPlaneRoutes(server);
    }

    // Root dir

    server.get({
        path: '/:account',
        name: 'GetRootDir',
        authAction: 'getdirectory'
    }, dir.rootDirHandler());

    server.head({
        path: '/:account',
        name: 'HeadRootDir',
        authAction: 'getdirectory'
    }, dir.rootDirHandler());

    server.put({
        path: '/:account',
        name: 'PutRootDir',
        authAction: 'putdirectory'
    }, dir.rootDirHandler());

    server.post({
        path: '/:account',
        name: 'PostRootDir'
    }, dir.rootDirHandler());

    server.del({
        path: '/:account',
        name: 'DeleteRootDir',
        authAction: 'deletedirectory'
    }, dir.rootDirHandler());


    /*
     * Here we generate routes for GET, PUT, HEAD, DELETE and OPTIONS requests
     * for all directory trees that are considered "storagePaths", which use
     * generic Muskie handlers (for example, PUT to a directory, GET an object,
     * and so on).
     */
    var storagePaths = common.storagePaths(options);
    Object.keys(storagePaths).forEach(function (k) {

        var _p = storagePaths[k].regex;
        var _n = storagePaths[k].name;

        // Otherwise in audit/dtrace we'll see GetStorageStorage
        if (_n === 'Storage')
            _n = '';

        server.put({
            path: _p,
            name: 'Put' + _n + 'Directory',
            contentType: 'application/json; type=directory',
            authAction: 'putdirectory'
        }, dir.putDirectoryHandler());

        server.put({
            path: _p,
            name: 'Put' + _n + 'Link',
            contentType: 'application/json; type=link',
            authAction: 'putlink'
        }, link.putLinkHandler());

        server.put({
            path: _p,
            name: 'Put' + _n + 'Object',
            contentType: '*/*',
            authAction: 'putobject'
        }, obj.putObjectHandler());

        server.opts({
            path: _p,
            name: 'Options' + _n + 'Storage'
        }, other.corsHandler());

        server.get({
            path: _p,
            name: 'Get' + _n + 'Storage'
        },  common.ensureEntryExistsHandler(),
            common.assertMetadataHandler(),
            dir.getDirectoryHandler(),
            obj.getObjectHandler());

        server.head({
            path: _p,
            name: 'Head' + _n + 'Storage'
        },  common.ensureEntryExistsHandler(),
            common.assertMetadataHandler(),
            dir.getDirectoryHandler(),
            obj.getObjectHandler());

        server.del({
            path: _p,
            name: 'Delete' + _n + 'Storage'
        },  common.ensureEntryExistsHandler(),
            common.assertMetadataHandler(),
            dir.deleteDirectoryHandler(),
            obj.deleteObjectHandler());
    });

    var _audit = audit.auditLogger({
        collector: options.collector,
        log: log
    });

    server.on('after', function (req, res, route, err) {
        _audit(req, res, route, err);

        if ((req.method === 'PUT' || req.method === 'POST') &&
            res.statusCode >= 400) {
            /*
             * An error occurred on a PUT or POST request, but there may still
             * be incoming data on the request stream. Call resume() in order to
             * dump any remaining request data so the stream emits an 'end' and
             * the socket resources are not leaked.
             */
            req.resume();
        }
    });

    return (server);
}


function forbiddenHandler(req, res, next) {
    req.log.debug('Method ' + req.method + ' disallowed for ' + req.url);
    res.send(405);
    next(false);
}


/*
 * This adds the routes for the majority of multipart upload API endpoints,
 * including:
 *   - create
 *   - get
 *   - abort
 *   - commit
 *
 * As well as handlers that redirect the client to the correct upload path if
 * they perform a request of the form:
 *    {HEAD,GET,PUT,POST} /:account/uploads/:id.
 *
 * Because 'uploads' is treated as a "storage path" (one of the top-level
 * storage directories in muskie), it automatically has routes created for it.
 * In some cases, we want to explicitly forbid these routes, so we do that here
 * and in addMultipartUploadDataPlaneRoutes.
 */
function addMultipartUploadRoutes(server) {

    /*
     * Path: /:account/uploads
     *
     * Allowed: POST (create-mpu), GET (list-mpu), HEAD
     * Disallowed: PUT, DELETE (automatically handled by commit/abort + GC)
     */
    server.post({
        path: '/:account/uploads',
        name: 'CreateUpload',
        contentType: 'application/json'
    }, uploads.createHandler());

    server.put({
        path: '/:account/uploads',
        name: 'PutUploads'
    }, forbiddenHandler);

    // Redirects
    /* JSSTYLED */
    var uploadsRedirectPath = '/:account/uploads/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}';
    /* JSSTYLED */
    var uploadsRedirectPathPart = '/:account/uploads/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/:partNum';
    server.get({
        path: uploadsRedirectPath,
        name: 'GetUploadRedirect',
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.put({
        path: uploadsRedirectPath,
        name: 'PutUploadRedirect',
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.head({
        path: uploadsRedirectPath,
        name: 'HeadUploadRedirect',
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.del({
        path: uploadsRedirectPath,
        name: 'DeleteUploadRedirect',
        contentType: '*/*'
    }, forbiddenHandler);

    server.post({
        path: uploadsRedirectPath,
        name: 'PostUploadRedirect',
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.get({
        path: uploadsRedirectPathPart,
        name: 'GetUploadPartRedirect',
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.put({
        path: uploadsRedirectPathPart,
        name: 'PutUploadPartRedirect',
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.head({
        path: uploadsRedirectPathPart,
        name: 'HeadUploadPartRedirect',
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.del({
        path: uploadsRedirectPathPart,
        name: 'DeleteUploadPartRedirect',
        contentType: '*/*'
    }, forbiddenHandler);

    server.post({
        path: uploadsRedirectPathPart,
        name: 'PostUploadPartRedirect',
        contentType: '*/*'
    }, uploads.redirectHandler());

    /*
     * Path: /:account/uploads/[0-f]/:id/state
     *
     * Allowed: GET (get-mpu)
     * Disallowed: HEAD, PUT, POST, DELETE
     */
    server.get({
        path: '/:account/uploads/[0-f]+/:id/state',
        name: 'GetUploadState'
    }, uploads.getHandler());

    server.head({
        path: '/:account/uploads/[0-f]+/:id/state',
        name: 'HeadUploadState'
    }, forbiddenHandler);

    server.put({
        path: '/:account/uploads/[0-f]+/:id/state',
        name: 'PutUploadState'
    }, forbiddenHandler);

    server.post({
        path: '/:account/uploads/[0-f]+/:id/state',
        name: 'PostUploadState'
    }, forbiddenHandler);

    server.del({
        path: '/:account/uploads/[0-f]+/:id/state',
        name: 'DeleteUploadState'
    }, forbiddenHandler);

    /*
     * Path: /:account/uploads/[0-f]/:id/abort
     *
     * Allowed: POST (abort-mpu)
     * Disallowed: GET, HEAD, PUT, DELETE
     */
    server.post({
        path: '/:account/uploads/[0-f]+/:id/abort',
        name: 'AbortUpload'
    }, uploads.abortHandler());

    server.get({
        path: '/:account/uploads/[0-f]+/:id/abort',
        name: 'GetAbortUpload'
    }, forbiddenHandler);

    server.put({
        path: '/:account/uploads/[0-f]+/:id/abort',
        name: 'PutAbortUpload'
    }, forbiddenHandler);

    server.head({
        path: '/:account/uploads/[0-f]+/:id/abort',
        name: 'HeadAbortUpload'
    }, forbiddenHandler);

    server.del({
        path: '/:account/uploads/[0-f]+/:id/abort',
        name: 'DeleteAbortUpload'
    }, forbiddenHandler);

    /*
     * Path: /:account/uploads/[0-f]/:id/commit
     *
     * Allowed: POST (commit-mpu)
     * Disallowed: GET, HEAD, PUT, DELETE
     */
    server.post({
        path: '/:account/uploads/[0-f]+/:id/commit',
        name: 'CommitUpload',
        contentType: 'application/json'
    }, uploads.commitHandler());

    server.get({
        path: '/:account/uploads/[0-f]+/:id/commit',
        name: 'GetCommitUpload'
    }, forbiddenHandler);

    server.put({
        path: '/:account/uploads/[0-f]+/:id/commit',
        name: 'PutCommitUpload'
    }, forbiddenHandler);

    server.head({
        path: '/:account/uploads/[0-f]+/:id/commit',
        name: 'HeadCommitUpload'
    }, forbiddenHandler);

    server.del({
        path: '/:account/uploads/[0-f]+/:id/commit',
        name: 'DeleteCommitUpload'
    }, forbiddenHandler);
}


/*
 * This adds the routes for the "data plane" portions of the multipart upload
 * API -- that is, the uploading of parts.
 */
function addMultipartUploadDataPlaneRoutes(server) {
    /*
     * Path: /:account/uploads/[0-f]/:id
     *
     * Allowed: GET (list-parts), HEAD
     * Disallowed: PUT, POST, DELETE
     */
    server.del({
        path: '/:account/uploads/[0-f]+/:id',
        name: 'DeleteUploadDir'
    }, forbiddenHandler);

    server.put({
        path: '/:account/uploads/[0-f]+/:id',
        name: 'PutUploadDir'
    }, forbiddenHandler);

    server.post({
        path: '/:account/uploads/[0-f]+/:id',
        name: 'PostUploadDir'
    }, forbiddenHandler);

    /*
     * Path: /:account/uploads/[0-f]/:id/:partNum
     *
     * Allowed: PUT (upload-part), HEAD
     * Disallowed: GET, POST, DELETE
     */
    server.del({
        path: '/:account/uploads/[0-f]+/:id/:partNum',
        name: 'DeletePart'
    }, forbiddenHandler);

    server.put({
        path: '/:account/uploads/[0-f]+/:id/:partNum',
        name: 'UploadPart',
        contentType: '*/*'
    }, uploads.uploadPartHandler());

    server.get({
        path: '/:account/uploads/[0-f]+/:id/:partNum',
        name: 'GetPart'
    }, forbiddenHandler);

    server.post({
        path: '/:account/uploads/[0-f]+/:id/:partNum',
        name: 'PostPart'
    }, forbiddenHandler);
}


///--- Exports

module.exports = {

    createServer: createServer,

    picker: picker,

    startKangServer: other.startKangServer,

    getMetricsHandler: other.getMetricsHandler
};
