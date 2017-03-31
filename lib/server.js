/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var crypto = require('crypto');
var fs = require('fs');
var path = require('path');
var url = require('url');

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
 * The returned server object will have a '.start()' method on it, which
 * wraps up the port/host settings for you.
 *
 * @arg {object} options      - options object.
 * @arg {string} options.file - configuration file to read from.
 * @arg {object} options.log  - bunyan logger.
 * @arg {function} callback   - of the form f(err, server).
 * @throws {TypeError} on bad input.
 */
function createServer(options, clearProxy) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

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

        if (!options.picker()) {
            req.log.error('picker unavailable');
            ok = false;
        } else if (!options.moray()) {
            req.log.error('index moray unavailable');
            ok = false;
        } else if (!options.mahi()) {
            req.log.error('mahi unavailable');
            ok = false;
        } else if (!options.marlin()) {
            req.log.error('marlin unavailable');
            ok = !req.isMarlinRequest();
        } else if (!options.medusa()) {
            req.log.error('medusa unavailable');
            ok = !req.isMedusaRequest();
        }

        if (!ok) {
            next(new ServiceUnavailableError());
        } else {
            next();
        }
    });

    server.use(auth.authenticationHandler({
        log: log,
        mahi: options.mahi,
        keyapi: options.keyapi
    }));

    server.use(auth.gatherContext);
    server.use(common.setupHandler(options));

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


    // Multipart Uploads
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

    // /jhendricks/uploads/:prefix/:id/:partNum
    //      - PUT
    //      - HEAD
    //      - Forbidden: GET/POST/DELETE
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

    // creates routes for everything in StoragePaths array
    // put___directory
    // put___link
    // put___object
    // options___storage
    // get___storage
    // head___storage
    // delete___storage
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
        log: log
    });

    server.on('uncaughtException', function (req, res, route, err) {
        if (!res._headerSent)
            res.send(err);

        _audit(req, res, route, err);
    });

    server.on('after', _audit);

    return (server);
}

function forbiddenHandler(req, res, next) {
        req.log.info('Method ' + req.method + ' disallowed for ' + req.url);
        res.send(403);
        next(false);
}


function addMultipartUploadRoutes(server) {
        server.post({
        path: '/:account/uploads',
        name: 'CreateUpload',
        contentType: 'application/json'
    }, uploads.createHandler());

    server.put({
        path: '/:account/uploads'
    }, forbiddenHandler);

    server.del({
        path: '/:account/uploads'
    }, forbiddenHandler);

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

    server.put({
        path: '/:account/uploads/[0-f]+/:id'
    }, forbiddenHandler);

    server.post({
        path: '/:account/uploads/[0-f]+/:id'
    }, forbiddenHandler);

    /* BEGIN JSSTYLED */
    /* server.del({
        path: '/:account/uploads/[0-f]+/:id'
    }, forbiddenHandler); */
    /* END JSSTYLED */

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

function addMultipartUploadDataPlaneRoutes(server) {
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

    server.del({
        path: '/:account/uploads/[0-f]+/:id/:partNum'
    }, forbiddenHandler);
}

///--- Exports

module.exports = {

    createServer: createServer,

    picker: picker,

    startKangServer: other.startKangServer
};
