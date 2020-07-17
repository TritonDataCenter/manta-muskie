/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var os = require('os');

var assert = require('assert-plus');
var artedi = require('artedi');
var restify = require('restify');
var restifyErrors = require('restify-errors');
var VError = require('verror').VError;

var common = require('./common');
var errors = require('./errors');



///--- Globals

var ForbiddenError = restifyErrors.ForbiddenError;
var UnauthorizedError = restifyErrors.UnauthorizedError;

var CONN_ID = 0;
var CROSSDOMAIN_XML =
    '<?xml version="1.0" encoding="UTF-8"?>\n' +
    '<cross-domain-policy' +
    ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' +
    /* JSSTYLED */
' xsi:noNamespaceSchemaLocation="http://www.adobe.com/xml/schemas/PolicyFile.xsd">\n' +
    '<cross-domain-policy>\n' +
    '    <allow-access-from domain="*" />\n' +
    '    <allow-http-request-headers-from domain="*" headers="*"/>\n' +
    '</cross-domain-policy>';
var CROSSDOMAIN_LEN = Buffer.byteLength(CROSSDOMAIN_XML);
var MAX_INT = Math.pow(2, 31) - 1;



///--- Stats
// We host all server stats here for both kang and dtrace probes

var STATS = {
    start: Date.now(),
    connections: {
    },
    requests: {
        methods: {
            DELETE: 0,
            GET: 0,
            HEAD: 0,
            POST: 0,
            PUT: 0,
            OPTIONS: 0
        },
        routes: {},
        previous: {
            total: 0,
            user_errors: 0,
            server_errors: 0
        }
    }
};
var STAT_TYPES = Object.keys(STATS);
STAT_TYPES.shift(); // drop 'start'

function kangListTypes() {
    return (STAT_TYPES);
}

function kangListObjects(type) {
    return (Object.keys(STATS[type]));
}


function kangGetObject(type, name) {
    return (STATS[type][name]);
}


function kangStats() {
    return (STATS);
}



///--- API

function flashXML(req, res, next) {
    res.set('Connection', 'keep-alive');
    res.set('Content-Length', CROSSDOMAIN_LEN);
    res.set('Content-Type', 'text/xml');
    res.set('Date', new Date());

    res.writeHead(200);
    res.write(CROSSDOMAIN_XML, 'utf8');
    res.end();

    next(false);
}


function logConnection(opts) {
    var log = opts.log;

    function _log(conn) {
        if (++CONN_ID >= MAX_INT)
            CONN_ID = 1;

        var addr = conn.remoteAddress;
        var log_obj = {
            id: CONN_ID,
            remoteAddress: addr,
            remotePort: conn.remotePort
        };

        if (!STATS.connections[addr]) {
            STATS.connections[addr] = 1;
        } else {
            STATS.connections[addr]++;
        }

        conn._muskie_id = CONN_ID;
        log.debug({
            connection: log_obj
        }, 'new connection');

        function onClose(had_err) {
            if (had_err) {
                log.warn({
                    connection: log_obj
                }, 'connection closed with error');
                conn._muskie_error = true;
            } else {
                log.debug({
                    connection: log_obj
                }, 'connection closed');
            }
            conn.removeListener('error', onError);

            if (STATS.connections[addr] !== undefined) {
                if (--STATS.connections[addr] <= 0)
                    delete STATS.connections[addr];
            }
        }

        function onError(err) {
            log.warn({
                err: err,
                connection: log_obj
            }, 'client connection error');
            conn._muskie_error = true;
            conn.removeListener('close', onClose);

            if (STATS.connections[addr] !== undefined) {
                if (--STATS.connections[addr] <= 0)
                    delete STATS.connections[addr];
            }
        }

        conn.once('close', onClose);
        conn.once('error', onError);
    }

    return (_log);
}


function pingHandler(clients) {
    assert.object(clients, 'clients');

    // Handler for "GET /ping".
    //
    // This is used by the HAProxy loadbalancer (aka "muppet") health check
    //      option httpchk GET /ping
    // to decide if this webapi process should be used. We only return 200
    // after this webapi has fully initialized. Currently the only async
    // init is setting `clients.moray` and `clients.picker|storinfo`.
    // Initializing Picker can take 10s of seconds which could, in slower cases,
    // lead to failing writes. See `function ensureDependencies` in "server.js"
    // for similar logic for checking if requests can be served.
    var ping = function ping(req, res, next) {
        res.set('Connection', 'close');
        res.set('Content-Length', 0);
        if (!(clients.moray && (clients.picker || clients.storinfo))) {
            res.writeHead(503);
        } else {
            res.writeHead(200);
        }
        res.end();
        next();
    };

    return (ping);
}


function redirect(req, res, next) {
    res.set('Content-Length', 0);
    res.set('Connection', 'keep-alive');
    res.set('Date', new Date());
    res.set('Location', 'http://apidocs.joyent.com/manta/');
    res.send(302);
    next(false);
}


function startKangServer() {
    var interval = 1000;
    function dtrace_fire() {

        STATS.requests.previous.total = 0;
        STATS.requests.previous.server_errors = 0;
        STATS.requests.previous.user_errors = 0;

        setTimeout(dtrace_fire, interval);
    }

    setTimeout(dtrace_fire, interval);

    // TODO: start one per muskie
    // var args = {
    //         uri_base: '/kang',
    //         port: 10080,
    //         service_name: 'muskie',
    //         version: '0.0.1',
    //         ident: os.hostname(),
    //         list_types: kangListTypes,
    //         list_objects: kangListObjects,
    //         get: kangGetObject,
    //         stats: kangStats
    // };
    // kang.knStartServer(args, assert.ifError.bind(assert));
}


function trackStartRequest(req, res, next) {
    var R = STATS.requests;
    if (R.methods[req.method] === undefined) {
        req.log.error('%s is unimplemented in stats tracker',
                      req.method);
        next();
        return;
    }

    if (!R.routes[req.route.name]) {
        R.routes[req.route.name] = 1;
    } else {
        R.routes[req.route.name]++;
    }

    R.methods[req.method]++;
    next();
}


// After event
function trackDoneRequest(req, res) {
    var R = STATS.requests;
    if (R.methods[req.method] === undefined)
        return;

    if (--R.methods[req.method] <= 0)
        R.methods[req.method] = 0;

    if (req.route && R.routes[req.route.name] !== undefined) {
        if (!--R.routes[req.route.name])
            delete R.routes[req.route.name];
    }

    if (res.statusCode >= 500) {
        STATS.requests.previous.server_errors++;
    } else if (res.statusCode >= 400) {
        STATS.requests.previous.user_errors++;
    }

    STATS.requests.previous.total++;
}


function mount(server, clients) {
    server.get({
        path: '/ping',
        name: 'GetPing'
    }, pingHandler(clients));

    // Docs
    server.get({
        path: '/',
        name: 'GetRoot'
    }, redirect);

    server.get({
        path: /^\/docs\/?/,
        name: 'GetDocs'
    }, redirect);

    // flash
    server.get({
        path:'/crossdomain.xml',
        name: 'GetCORS'
    }, flashXML);
}


//--- CORS
//
// http://www.w3.org/TR/cors/#resource-preflight-requests
// https://developer.mozilla.org/en-US/docs/HTTP/Access_control_CORS
//
// CORS is about the most complicated HTTP spec in existence.  All the official
// rules are on the W3C page, but Manta adds more complexity as we want
// non-existing entities to infer the CORS rules from the parent directory on
// PUT requests (otherwise, browsers can't write).
//
// As such we have to handle OPTIONS requests differently depending on whether
// Access-Control-Allow-Method is sent
//

function preflightPUTRequest(req, res, next) {
    // hack-a-ma-hack: use directory metadata to control writes
    if (req.headers['access-control-request-method'] === 'PUT')
        req.metadata = req.parentMetadata;

    next();
}


function preflightRequest(req, res, next) {
    var log = req.log;
    var headers = req.headers;
    var md = (req.metadata || {}).headers || {};

    var allowOrigin = md['access-control-allow-origin'];
    var allowMethod = md['access-control-allow-methods'];
    var allowHeaders = md['access-control-allow-headers'] || [];
    var origin = headers.origin;
    var _headers = (headers['access-control-request-headers'] || '').
        /* JSSTYLED */
    split(/\s*,\s*/);
    var method = headers['access-control-request-method'];

    log.debug({
        'access-control-request-method': method,
        'access-control-request-headers': _headers,
        md: md,
        origin: origin
    }, 'preflightRequest: entered');

    assert.equal(req.method, 'OPTIONS');

    if (!origin || !method || !allowOrigin || !allowMethod ||
        /* JSSTYLED */
        !allowOrigin.split(/\s*,\s*/).some(function (v) {
            if (v === req.headers.origin || v === '*') {
                return (true);
            }
            return (false);
        }) ||
        /* JSSTYLED */
        !allowMethod.split(/\s*,\s*/).some(function (v) {
            if (v === req.headers['access-control-request-method']) {
                return (true);
            }
            return (false);
        }) ||
        (_headers.length && !_headers.every(function (v) {
            return (!v || allowHeaders.indexOf(v) !== -1);
        }))) {
        res.send(200);
        next();
        return;
    }


    res.header('Access-Control-Allow-Origin', origin);
    res.header('Access-Control-Allow-Methods', allowMethod);
    if (allowHeaders)
        res.header('Access-Control-Allow-Headers', allowHeaders);
    if (md['access-control-max-age']) {
        res.header('Access-Control-Max-Age',
                   md['access-control-max-age']);
    }

    res.send(200);
    next();
}


///--- Exports

module.exports = {
    logConnection: logConnection,
    mount: mount,
    startKangServer: startKangServer,
    trackStartRequest: trackStartRequest,
    trackDoneRequest: trackDoneRequest,

    corsHandler: function corsHandler() {
        var chain = [
            restify.plugins.conditionalRequest(),
            common.ensureParentHandler(),
            preflightPUTRequest,
            common.ensureEntryExistsHandler(),
            preflightRequest
        ];

        return (chain);
    },
    getMetricsHandler: function metricsHandler(collector) {
        var chain = [
            function getMetrics(req, res, next) {
                req.on('end', function () {
                    collector.collect(artedi.FMT_PROM, function (err, metrics) {
                        if (err) {
                            next(new VError(err, 'error retrieving metrics'));
                            return;
                        }
                        res.setHeader('Content-Type',
                            'text/plain; version=0.0.4');
                        res.send(metrics);
                        next();
                    });
                });
                req.resume();
            }
        ];
        return (chain);
    }
};
