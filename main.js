/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var fs = require('fs');
var net = require('net');
var os = require('os');
var path = require('path');

var apertureConfig = require('aperture-config').config;
var artedi = require('artedi');
var assert = require('assert-plus');
var bsyslog = require('bunyan-syslog');
var bunyan = require('bunyan');
var cueball = require('cueball');
var dashdash = require('dashdash');
var dtrace = require('dtrace-provider');
var jsprim = require('jsprim');
var kang = require('kang');
var keyapi = require('keyapi');
var libmanta = require('libmanta');
var LRU = require('lru-cache');
var mahi = require('mahi');
var marlin = require('marlin');
var medusa = require('./lib/medusa');
var once = require('once');
var restify = require('restify');
var vasync = require('vasync');

var app = require('./lib');
var uploadsCommon = require('./lib/uploads/common');


///--- Internal Functions

function getOptions() {
    var options = [
        {
            names: ['file', 'f'],
            type: 'string',
            help: 'Configuration file to use.',
            helpArg: 'FILE'
        },
        {
            names: ['insecure-port', 'i'],
            type: 'positiveInteger',
            help: 'Listen for insecure requests on port.',
            helpArg: 'PORT'
        },
        {
            names: ['port', 'p'],
            type: 'positiveInteger',
            help: 'Listen for secure requests on port.',
            helpArg: 'PORT'
        },
        {
            names: ['verbose', 'v'],
            type: 'arrayOfBool',
            help: 'Verbose output. Use multiple times for more verbose.'
        }
    ];

    return (options);
}


function configure(appName, dtProbes) {
    var cfg, opts;
    var parser = new dashdash.Parser({options: getOptions()});

    var log = bunyan.createLogger(
        {
            name: appName,
            streams: [ {
                level: process.env.LOG_LEVEL || 'info',
                stream: process.stderr
            } ],
            serializers: restify.bunyan.serializers
        });

    if (log.level() <= bunyan.DEBUG) {
        log = log.child({src: true});
    }

    try {
        opts = parser.parse(process.argv);
        assert.object(opts, 'options');
    } catch (e) {
        log.fatal(e, 'invalid options');
        usage(parser, e.message);
    }

    if (!opts.file) {
        usage(parser, '-f option is required');
    }

    cfg = JSON.parse(readFile(opts.file));
    cfg.insecurePort = opts.insecure_port || cfg.insecurePort;
    cfg.port = opts.port || cfg.port;

    cfg.name = 'Manta';
    cfg.version = version();

    log = configureLogging(appName, opts, cfg, log);

    if (opts.verbose) {
        opts.verbose.forEach(function () {
            log.level(Math.max(bunyan.TRACE, (log.level() - 10)));
        });
    }

    if (!cfg.hasOwnProperty('storage')) {
        cfg.storage = {};
    }

    /*
     * For streaming PUTs and multi-part uploads, we may not know the full size
     * of the object until the completion of the request.  For object
     * placement, we must know in advance the maximum expected size of the
     * stream.  If the client does not provide a "Max-Content-Length" header,
     * we assume a default value.  An operator may override this value by using
     * the "MUSKIE_DEFAULT_MAX_STREAMING_SIZE_MB" SAPI property.
     */
    if (cfg.storage.hasOwnProperty('defaultMaxStreamingSizeMB')) {
        var v = cfg.storage.defaultMaxStreamingSizeMB;

        /*
         * The structure of the configuration template is such that the value
         * is a valid Number or not present at all.  Any other case would have
         * already caused a JSON parse failure at an earlier point in this
         * function.
         */
        if (typeof (v) !== 'number' || v < 1) {
            log.fatal('invalid "defaultMaxStreamingSizeMB" value');
            process.exit(1);
        }
    } else {
        cfg.storage.defaultMaxStreamingSizeMB = 51200;
    }

    if (!cfg.hasOwnProperty('multipartUpload')) {
        cfg.multipartUpload = {};
    }

    if (cfg.multipartUpload.hasOwnProperty('prefixDirLen')) {
        var len = cfg.multipartUpload.prefixDirLen;
        assert.number(len, '"prefixDirLen" value must be a number');

        if (len < uploadsCommon.MIN_PREFIX_LEN ||
            len > uploadsCommon.MAX_PREFIX_LEN) {

            log.fatal('invalid "prefixDirLen" value: must be between ' +
                uploadsCommon.MIN_PREFIX_LEN + ' and ' +
                uploadsCommon.MAX_PREFIX_LEN);
            process.exit(1);
        }
    } else {
        cfg.multipartUpload.prefixDirLen = uploadsCommon.DEF_PREFIX_LEN;
    }

    cfg.collector = artedi.createCollector({
        labels: {
            datacenter: cfg.datacenter,
            server: cfg.server_uuid,
            zonename: cfg.zone_uuid,
            pid: process.pid
        }
    });

    cfg.jobCache = LRU({
        maxAge: (cfg.marlin.jobCache.expiry * 1000) || 30000,
        max: cfg.marlin.jobCache.size || 1000
    });

    cfg.dtrace_probes = dtProbes;

    log.debug(cfg, 'muskie: config loaded');

    return (cfg);
}


function configureLogging(appName, opts, cfg, log) {
    // This is ugly, but we set this up so that if muskie is invoked with a
    // -v flag, then we know you're running locally, and you just want the
    // messages to spew to stdout. Otherwise, it's "production", and muskie
    // logs to a syslog endpoint

    if (opts.verbose || !cfg.bunyan) {
        setLogger(cfg, log);
        return (cfg);
    }

    var cfg_b = cfg.bunyan;

    assert.object(cfg_b, 'config.bunyan');
    assert.optionalString(cfg_b.level, 'config.bunyan.level');
    assert.optionalObject(cfg_b.syslog, 'config.bunyan.syslog');

    var level = cfg_b.level || 'info';
    var streams = [];
    var sysl;

    // We want debug info only IFF the request fails AND the configured
    // log level is info or higher
    if (bunyan.resolveLevel(level) >= bunyan.INFO && cfg_b.syslog) {
        assert.string(cfg_b.syslog.facility,
                      'config.bunyan.syslog.facility');
        assert.string(cfg_b.syslog.type, 'config.bunyan.syslog.type');

        const RequestCaptureStream = restify.bunyan.RequestCaptureStream;

        sysl = bsyslog.createBunyanStream({
            facility: bsyslog.facility[cfg_b.syslog.facility],
            name: appName,
            host: cfg_b.syslog.host,
            port: cfg_b.syslog.port,
            type: cfg_b.syslog.type
        });
        streams.push({
            level: level,
            stream: sysl
        });
        streams.push({
            level: 'debug',
            type: 'raw',
            stream: new RequestCaptureStream({
                level: bunyan.WARN,
                maxRecords: 2000,
                maxRequestIds: 2000,
                streams: [ {
                    raw: true,
                    stream: sysl
                }]
            })
        });
        log = bunyan.createLogger({
            name: appName,
            level: level,
            streams: streams,
            serializers: restify.bunyan.serializers
        });
    }

    setLogger(cfg, log);

    return (log);
}


function setLogger(cfg, log) {
    assert.object(cfg, 'cfg');
    assert.object(cfg.auth, 'cfg.auth');
    assert.object(cfg.marlin, 'cfg.marlin');
    assert.object(cfg.moray, 'cfg.moray');
    assert.object(cfg.medusa, 'cfg.medusa');
    assert.object(cfg.cueballHttpAgent, 'cfg.cueballHttpAgent');
    assert.object(cfg.sharkConfig, 'cfg.sharkConfig');

    cfg.log = log;
    cfg.auth.log = log;
    cfg.marlin.log = log;
    cfg.moray.log = log;
    cfg.medusa.log = log;
    cfg.cueballHttpAgent.log = log;
    cfg.sharkConfig.log = log;
}


function usage(parser, message)
{
    console.error('muskie: %s', message);
    console.error('usage: node main.js OPTIONS\n');
    console.error(parser.help());
    process.exit(2);
}

function createMonitoringServer(cfg) {
    /*
     * Set up the monitoring server. This exposes a cueball kang monitoring
     * listener and an artedi-based metric collector.
     *
     * The cueball monitoring listener serves information about the cueball
     * Pools and Sets for connections to mahi, sharks, other services, and also
     * the moray client connections.
     *
     * The artedi-based metric collector is used to track various muskie
     * metrics including operation latency, and request counts.
     */
    var kangOpts;
    var monitorServer;
    var port;
    kangOpts = cueball.poolMonitor.toKangOptions();
    port = cfg.port + 800;

    monitorServer = restify.createServer({ serverName: 'Monitor' });
    monitorServer.get('/metrics', app.getMetricsHandler(cfg.collector));
    monitorServer.get(new RegExp('.*'), kang.knRestifyHandler(kangOpts));

    monitorServer.listen(port, '0.0.0.0', function () {
        cfg.log.info('monitoring server started on port %d', port);
    });
}

function createCueballHttpAgent(sharkCfg, cueballHttpAgent, clients) {
    /* Used for connections to mahi and other services. */
    clients.agent = new cueball.HttpAgent(cueballHttpAgent);

    /* Used only for connections to sharks. */
    var sharkCueball = {
        resolvers: sharkCfg.resolvers,

        spares: sharkCfg.spares,
        maximum: sharkCfg.maximum,
        /*
         * Note that this path doesn't actually have to be handled by the
         * authcache (any non-5xx response code is accepted, e.g. 404 is fine).
         */
        ping: sharkCfg.ping,
        pingInterval: sharkCfg.pingInterval,
        tcpKeepAliveInitialDelay: sharkCfg.maxIdleTime,

        log: sharkCfg.log,
        recovery: {
            default: {
                retries: sharkCfg.retry.retries,
                timeout: sharkCfg.connectTimeout,
                maxTimeout: sharkCfg.maxTimeout,
                delay: sharkCfg.delay
            },
            /*
             * Avoid SRV retries, since authcache doesn't currently register
             * any useable SRV records for HTTP (it only registers redis)
             */
            'dns_srv': {
                retries: 0,
                timeout: sharkCfg.connectTimeout,
                maxTimeout: sharkCfg.maxTimeout,
                delay: 0
            }
        }
    };
    clients.sharkAgent = new cueball.HttpAgent(sharkCueball);
}

function createPickerClient(cfg, log, clients, barrier) {
    barrier.start('createPickerClient');

    var opts = {
        interval: cfg.interval,
        lag: cfg.lag,
        moray: cfg.moray,
        log: log.child({component: 'picker'}, true),
        multiDC: cfg.multiDC,
        defaultMaxStreamingSizeMB: cfg.defaultMaxStreamingSizeMB,
        maxUtilizationPct: cfg.maxUtilizationPct || 90
    };

    var client = app.picker.createClient(opts);

    client.once('connect', function onConnect() {
        log.info('picker connected %s', client.toString());
        clients.picker = client;

        barrier.done('createPickerClient');
    });
}


function createAuthCacheClient(authCfg, clients) {
    assert.object(authCfg, 'authCfg');
    assert.string(authCfg.url, 'authCfg.url');
    assert.optionalObject(authCfg.typeTable, 'authCfg.typeTable');

    var options = jsprim.deepCopy(authCfg);
    var log = authCfg.log.child({component: 'mahi'}, true);
    options.log = log;

    options.typeTable = options.typeTable || apertureConfig.typeTable || {};
    options.agent = clients.agent;

    clients.mahi = mahi.createClient(options);
}


function createKeyAPIClient(opts, clients) {
    var log = opts.log.child({component: 'keyapi'}, true);
    var _opts = {
        log: log,
        ufds: opts.ufds
    };
    clients.keyapi = new keyapi(_opts);
}


function createMarlinClient(opts, clients) {
    var log = opts.log.child({component: 'marlin'}, true);
    var _opts = {
        moray: opts.moray,
        setup_jobs: true,
        log: log
    };

    marlin.createClient(_opts, function (err, marlinClient) {
        if (err) {
            log.fatal(err, 'marlin: unable to create a client');
            process.nextTick(createMarlinClient.bind(null, opts));
        } else {
            clients.marlin = marlinClient;
            log.info({
                remote: marlinClient.ma_client.host
            }, 'marlin: ready');

            /*
             * In general, for various failures, we should get both an 'error'
             * and a 'close' event.  We want to wait for both so that we don't
             * start reconnecting while the first client is still connected.  (A
             * persistent error could result in way too many Moray connections.)
             */
            var barrier = vasync.barrier();
            barrier.start('wait-for-close');
            marlinClient.on('close', function () {
                barrier.done('wait-for-close');
            });

            barrier.start('wait-for-error');
            marlinClient.on('error', function (marlin_err) {
                log.error(marlin_err, 'marlin error');
                barrier.done('wait-for-error');
                marlinClient.close();
            });

            barrier.on('drain', function doReconnect() {
                log.info('marlin: reconnecting');
                createMarlinClient(opts);
            });
        }
    });
}


function createMorayClient(opts, clients, barrier) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');

    barrier.start('createMorayClient');

    var log = opts.log.child({component: 'moray'}, true);
    opts.log = log;

    var client = new libmanta.createMorayClient(opts);

    client.once('error', function (err) {
        client.removeAllListeners('connect');

        log.error(err, 'moray: failed to connect');
    });

    client.once('connect', function onConnect() {
        client.removeAllListeners('error');

        log.info({
            host: opts.host,
            port: opts.port
        }, 'moray: connected');

        clients.moray = client;

        barrier.done('createMorayClient');
    });
}


function createMedusaConnector(opts, clients) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');

    var log = opts.log.child({ component: 'medusa' });
    log.debug({ opts: opts }, 'medusa options');

    var client = medusa.createConnector(opts);

    client.once('connect', function onConnect() {
        log.info('medusa: connected');
        clients.medusa = client;
    });
}


function readFile(file) {
    var data;

    try {
        data = fs.readFileSync(file, 'utf8');
    } catch (e) {
        console.error('Unable to load %s: %s', file, e.message);
        process.exit(1);
    }

    return (data);
}


function version() {
    var fname = __dirname + '/package.json';
    var pkg = fs.readFileSync(fname, 'utf8');
    return (JSON.parse(pkg).version);
}


function clientsConnected(appName, cfg, clients) {
    var server1, server2;
    var log = cfg.log;

    log.info('requisite client connections established, ' +
             'starting muskie servers');

    server1 = app.createServer(cfg, clients, 'ssl');
    server1.on('error', function (err) {
        log.fatal(err, 'server (secure) error');
        process.exit(1);
    });
    server1.listen(cfg.port, function () {
        log.info('%s listening at (trusted port) %s', appName, server1.url);
    });

    server2 = app.createServer(cfg, clients, 'insecure');
    server2.on('error', function (err) {
        log.fatal(err, 'server (clear) error');
        process.exit(1);
    });
    server2.listen(cfg.insecurePort, function () {
        log.info('%s listening at (clear port) %s', appName, server2.url);
    });

    app.startKangServer();
}


///--- Mainline

(function main() {
    const muskie = 'muskie';

    // Parent object for client connection objects
    var clients = {};

    // DTrace probe setup
    var dtp = dtrace.createDTraceProvider(muskie);
    var client_close = dtp.addProbe('client_close', 'json');
    var socket_timeout = dtp.addProbe('socket_timeout', 'json');

    client_close.dtp = dtp;
    socket_timeout.dtp = dtp;
    dtp.enable();

    const dtProbes = {
        client_close: client_close,
        socket_timeout: socket_timeout
    };

    // Do not mutate config data
    const cfg = configure(muskie, dtProbes);

    // Create a barrier to ensure client connections that are
    // established asynchronously and are required for muskie to serve
    // requests are ready prior to starting up the restify servers and
    // beginning to handle requests.
    var barrier = vasync.barrier();

    barrier.on('drain', clientsConnected.bind(null, muskie, cfg, clients));

    // Establish client connections
    createCueballHttpAgent(cfg.sharkConfig, cfg.cueballHttpAgent, clients);
    createMonitoringServer(cfg);
    createMarlinClient(cfg.marlin, clients);
    createPickerClient(cfg.storage, cfg.log, clients, barrier);
    createAuthCacheClient(cfg.auth, clients);
    createMorayClient(cfg.moray, clients, barrier);
    createMedusaConnector(cfg.medusa, clients);
    createKeyAPIClient(cfg, clients);

    process.on('SIGHUP', process.exit.bind(process, 0));

})();
