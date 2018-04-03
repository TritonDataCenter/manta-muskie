/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

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
var jobs = require('./jobs');
var link = require('./link');
var medusa = require('./medusa');
var obj = require('./obj');
var other = require('./other');
var picker = require('./picker');
var uploads = require('./uploads');
var throttle = require('./throttle');

// injects into the global namespace
require('./errors');



///--- Globals

var JOBS_ROOT_RE = /^\/([a-zA-Z][a-zA-Z0-9_\.@%]+)\/jobs\/?$/;
/* JSSTYLED */
var JOBS_LIVE_RE = /(state|status|name)=/i;


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
    // A histogram to track the time to first byte.
    options.collector.histogram({
        name: common.METRIC_LATENCY_HISTOGRAM,
        help: 'time-to-first-byte of Muskie requests'
    });
    // A histogram to track the time it took to fully process each HTTP request.
    options.collector.histogram({
        name: common.METRIC_DURATION_HISTOGRAM,
        help: 'total time to process Muskie requests'
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
        /* JSSTYLED */
        if (ct && !/.*\/.*/.test(ct))
            req.headers['content-type'] = mime.lookup(ct);
        next();
    });

    server.pre(function routeLiveJobs(req, res, next) {
        var tmp = JOBS_ROOT_RE.exec(req.path());
        if (tmp && tmp.length > 1 && JOBS_LIVE_RE.test(req.query()))
            req._path = '/' + tmp[1] + '/jobs/live';

        next();
    });

    // set up random stuff
    other.mount(server);

    server.use(common.earlySetupHandler(options));
    server.use(restify.dateParser(options.maxRequestAge || 300));
    server.use(restify.queryParser());
    server.use(common.authorizationParser);
    server.use(auth.checkIfPresigned);
    server.use(common.enforceSSLHandler(options));

    server.use(function ensureDependencies(req, res, next) {
        var ok = true;
        var errors = [];

        // if (!clients.picker) {
        //     errors.push(new Error('picker unavailable'));
        //     req.log.error('picker unavailable');
        //     ok = false;
        if (!clients.moray) {
            errors.push(new Error('index moray unavailable'));
            req.log.error('index moray unavailable');
            ok = false;
        } else if (!clients.mahi) {
            errors.push(new Error('mahi unavailable'));
            req.log.error('mahi unavailable');
            ok = false;
        } else if (!clients.marlin) {
            errors.push(new Error('marlin available'));
            req.log.error('marlin unavailable');
            ok = !req.isMarlinRequest();
        } else if (!clients.medusa) {
            errors.push(new Error('medusa unavailable'));
            req.log.error('medusa unavailable');
            ok = !req.isMedusaRequest();
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

    // Compute jobs

    server.post({
        path: '/:account/jobs',
        name: 'CreateJob'
    }, jobs.createHandler());

    server.get({
        path: '/:account/jobs/live',
        name: 'ListJobs'
    }, jobs.listHandler());

    server.get({
        path: '/:account/jobs/:id/live/status',
        name: 'GetJobStatus',
        authAction: 'getjob'
    }, jobs.getHandler());

    server.post({
        path: '/:account/jobs/:id/live/cancel',
        name: 'PostJobCancel',
        authAction: 'managejob'
    }, jobs.cancelHandler());

    server.get({
        path: '/:account/jobs/:id/live/err',
        name: 'GetJobErrors',
        authAction: 'getjob'
    }, jobs.getErrorsHandler());

    server.get({
        path: '/:account/jobs/:id/live/fail',
        name: 'GetJobFailures',
        authAction: 'getjob'
    }, jobs.getFailuresHandler());

    server.post({
        path: '/:account/jobs/:id/live/in',
        name: 'PostJobInput',
        authAction: 'managejob'
    }, jobs.addInputHandler());

    server.get({
        path: '/:account/jobs/:id/live/in',
        name: 'GetJobInput',
        authAction: 'getjob'
    }, jobs.getInputHandler());

    server.post({
        path: '/:account/jobs/:id/live/in/end',
        name: 'PostJobInputDone',
        authAction: 'managejob'
    }, jobs.endInputHandler());

    server.get({
        path: '/:account/jobs/:id/live/out',
        name: 'GetJobOutput',
        authAction: 'getjob'
    }, jobs.getOutputHandler());


    server.get({
        path: '/:account/medusa/attach/:id/:type',
        name: 'MedusaAttach',
        authAction: 'mlogin'
    }, medusa.getMedusaAttachHandler());


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

    server.get({path: '/:account/jobs', name: 'ListJobs'},
               common.getMetadataHandler(),
               common.ensureEntryExistsHandler(),
               common.assertMetadataHandler(),
               dir.getDirectoryHandler());

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

    server.on('uncaughtException', function (req, res, route, err) {
        if (!res._headerSent)
            res.send(err);

        _audit(req, res, route, err);
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
 *    {HEAD,GET,PUT,POST,DELETE} /:account/uploads/:id.
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
     * Disallowed: PUT, DELETE (automatically handled)
     */
    server.post({
        path: '/:account/uploads',
        name: 'CreateUpload',
        contentType: 'application/json'
    }, uploads.createHandler());

    server.put({
        path: '/:account/uploads'
    }, forbiddenHandler);

    // Redirects
    /* JSSTYLED */
    var uploadsRedirectPath = '/:account/uploads/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}';
    /* JSSTYLED */
    var uploadsRedirectPathPart = '/:account/uploads/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/:partNum';
    server.get({
        path: uploadsRedirectPath,
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.put({
        path: uploadsRedirectPath,
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.head({
        path: uploadsRedirectPath,
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.del({
        path: uploadsRedirectPath,
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.post({
        path: uploadsRedirectPath,
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.get({
        path: uploadsRedirectPathPart,
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.put({
        path: uploadsRedirectPathPart,
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.head({
        path: uploadsRedirectPathPart,
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.del({
        path: uploadsRedirectPathPart,
        contentType: '*/*'
    }, uploads.redirectHandler());

    server.post({
        path: uploadsRedirectPathPart,
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
        name: 'GetUpload'
    }, uploads.getHandler());

    server.head({
        path: '/:account/uploads/[0-f]+/:id/state'
    }, forbiddenHandler);

    server.put({
        path: '/:account/uploads/[0-f]+/:id/state'
    }, forbiddenHandler);

    server.post({
        path: '/:account/uploads/[0-f]+/:id/state'
    }, forbiddenHandler);

    server.del({
        path: '/:account/uploads/[0-f]+/:id/state'
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
        path: '/:account/uploads/[0-f]+/:id/abort'
    }, forbiddenHandler);

    server.put({
        path: '/:account/uploads/[0-f]+/:id/abort'
    }, forbiddenHandler);

    server.head({
        path: '/:account/uploads/[0-f]+/:id/abort'
    }, forbiddenHandler);

    server.del({
        path: '/:account/uploads/[0-f]+/:id/abort'
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
        path: '/:account/uploads/[0-f]+/:id/commit'
    }, forbiddenHandler);

    server.put({
        path: '/:account/uploads/[0-f]+/:id/commit'
    }, forbiddenHandler);

    server.head({
        path: '/:account/uploads/[0-f]+/:id/commit'
    }, forbiddenHandler);

    server.del({
        path: '/:account/uploads/[0-f]+/:id/commit'
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
     * Disallowed: PUT, POST, DELETE (except with override query param)
     */
    server.del({
        path: '/:account/uploads/[0-f]+/:id',
        name: 'DelUploadDir'
    }, uploads.delUploadDirHandler());

    server.put({
        path: '/:account/uploads/[0-f]+/:id'
    }, forbiddenHandler);

    server.post({
        path: '/:account/uploads/[0-f]+/:id'
    }, forbiddenHandler);

    /*
     * Path: /:account/uploads/[0-f]/:id/:partNum
     *
     * Allowed: PUT (upload-part), HEAD
     * Disallowed: GET, POST, DELETE (except with override query param)
     */
    server.del({
        path: '/:account/uploads/[0-f]+/:id/:partNum',
        name: 'DelPart'
    }, uploads.delPartHandler());

    server.put({
        path: '/:account/uploads/[0-f]+/:id/:partNum',
        name: 'UploadPart',
        contentType: '*/*'
    }, uploads.uploadPartHandler());

    server.get({
        path: '/:account/uploads/[0-f]+/:id/:partNum'
    }, forbiddenHandler);

    server.post({
        path: '/:account/uploads/[0-f]+/:id/:partNum'
    }, forbiddenHandler);
}


///--- Exports

module.exports = {

    createServer: createServer,

    picker: picker,

    startKangServer: other.startKangServer,

    getMetricsHandler: other.getMetricsHandler
};
