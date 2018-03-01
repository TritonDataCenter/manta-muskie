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
var rethinkdb = require('rethinkdb');
var vasync = require('vasync');

var app = require('./lib');
var uploadsCommon = require('./lib/uploads/common');


///--- Internal Functions

function getMuskieOptions() {
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


/**
 * Command line option parsing and checking.
 *
 * @returns {Object} A object representing the command line options.
 */
function parseOptions() {
    var opts;
    var parser = new dashdash.Parser({options: getMuskieOptions()});

    try {
        opts = parser.parse(process.argv);
        assert.object(opts, 'options');
    } catch (e) {
        usage(parser, e.message);
    }

    if (!opts.file) {
        usage(parser, '-f option is required');
    }

    return (opts);
}


/**
 * Configure the application based on the configuration file data and the
 * command line options.
 *
 * @param {String} appName: Required. The name of the application.
 * @param {Object} opts: Required. An object representing the parsed command
 * line options.
 * @param {} dtProbes: Required. An object containing the dtrace probes for the
 * application.
 * @returns {Object} The configuration object.
 */
function configure(appName, opts, dtProbes) {
    var cfg = JSON.parse(readFile(opts.file));

    assert.object(cfg, 'cfg');
    assert.object(cfg.auth, 'cfg.auth');
    assert.object(cfg.marlin, 'cfg.marlin');
    assert.object(cfg.moray, 'cfg.moray');
    assert.object(cfg.medusa, 'cfg.medusa');
    assert.object(cfg.cueballHttpAgent, 'cfg.cueballHttpAgent');
    assert.object(cfg.sharkConfig, 'cfg.sharkConfig');
    assert.optionalObject(cfg.storage, 'cfg.storage');

    cfg.insecurePort = opts.insecure_port || cfg.insecurePort;
    cfg.port = opts.port || cfg.port;
    cfg.name = 'Manta';
    cfg.version = version();
    cfg.log = configureLogging(appName, cfg.bunyan, opts.verbose);

    [ cfg.auth, cfg.marlin, cfg.moray, cfg.medusa,
      cfg.cueballHttpAgent, cfg.sharkConfig
    ].forEach(function (x) {
        x.log = cfg.log;
    });

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
            cfg.log.fatal('invalid "defaultMaxStreamingSizeMB" value');
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

            cfg.log.fatal('invalid "prefixDirLen" value: must be between ' +
                uploadsCommon.MIN_PREFIX_LEN + ' and ' +
                uploadsCommon.MAX_PREFIX_LEN);
            process.exit(1);
        }
    } else {
        cfg.multipartUpload.prefixDirLen = uploadsCommon.DEF_PREFIX_LEN;
    }

    if (cfg.storage.hasOwnProperty('metadataBackend')) {
        var metadataBackend = cfg.storage.metadataBackend;

        if (typeof (metadataBackend) !== 'string') {
            cfg.log.fatal('invalid "metadataBackend" value');
            process.exit(1);
        }
    } else {
        cfg.storage.metadataBackend = 'moray';
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

    cfg.log.debug(cfg, 'muskie: config loaded');

    return (cfg);
}


/**
 * Configure the logger based on the configuration data
 *
 * @param {String} appName: Required. The name of the application
 * @param {Object} bunyanCfg: Required. The bunyan configuration data
 * @param {arrayOfBool} verbose: Required. Array of boolean values that
 * indicates if verbose logging should be enabled and the level of verbosity
 * requested. Used to determine if syslog logging should be configured or not
 * and to set the logging level appropriately.
 * @returns {Object} A bunyan logger object
 */
function configureLogging(appName, bunyanCfg, verbose) {
    var log = bunyan.createLogger({
        name: appName,
        streams: [ {
            level: process.env.LOG_LEVEL || 'info',
            stream: process.stderr
        } ],
        serializers: restify.bunyan.serializers
    });
    var level;

    /*
     * This is ugly, but we set this up so that if muskie is invoked with a
     * -v flag, then we know you're running locally, and you just want the
     * messages to spew to stderr. Otherwise, it's "production", and muskie
     * likely logs to a syslog endpoint.
     */
    if (verbose || !bunyanCfg) {
        if (verbose) {
            level = Math.max(bunyan.TRACE, (log.level() - verbose.length * 10));
            log.level(level);
        }

        return (log);
    }

    assert.object(bunyanCfg, 'config.bunyan');
    assert.optionalString(bunyanCfg.level, 'config.bunyan.level');
    assert.optionalObject(bunyanCfg.syslog, 'config.bunyan.syslog');

    level = bunyan.resolveLevel(bunyanCfg.level || 'info');

    if (bunyanCfg.syslog) {
        var streams = [];
        var sysl;

        assert.string(bunyanCfg.syslog.facility,
                      'config.bunyan.syslog.facility');
        assert.string(bunyanCfg.syslog.type, 'config.bunyan.syslog.type');

        sysl = bsyslog.createBunyanStream({
            facility: bsyslog.facility[bunyanCfg.syslog.facility],
            name: appName,
            host: bunyanCfg.syslog.host,
            port: bunyanCfg.syslog.port,
            type: bunyanCfg.syslog.type
        });

        streams.push({
            level: level,
            stream: sysl
        });

        /*
         * We want debug info only IFF the request fails AND the configured
         * log level is info or higher.
         */
        if (level >= bunyan.INFO) {
            const RequestCaptureStream = restify.bunyan.RequestCaptureStream;
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
        }

        log = bunyan.createLogger({
            name: appName,
            level: level,
            streams: streams,
            serializers: restify.bunyan.serializers
        });
    } else {
        log.level(level);
    }

    if (log.level() <= bunyan.DEBUG) {
        log = log.child({src: true});
    }

    return (log);
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


function createCueballSharkAgent(sharkCfg) {
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
             * any useable SRV records for HTTP (it only registers redis).
             */
            'dns_srv': {
                retries: 0,
                timeout: sharkCfg.connectTimeout,
                maxTimeout: sharkCfg.maxTimeout,
                delay: 0
            }
        }
    };

    return (new cueball.HttpAgent(sharkCueball));
}


function onPickerConnect(clients, barrier, pickerClient) {
    clients.picker = pickerClient;
    barrier.done('createPickerClient');
}


function createPickerClient(cfg, log, onConnect) {
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

    client.once('connect', function _onConnect() {
        log.info('picker connected %s', client.toString());
        onConnect(client);
    });
}


function createAuthCacheClient(authCfg, agent) {
    assert.object(authCfg, 'authCfg');
    assert.string(authCfg.url, 'authCfg.url');
    assert.optionalObject(authCfg.typeTable, 'authCfg.typeTable');

    var options = jsprim.deepCopy(authCfg);
    var log = authCfg.log.child({component: 'mahi'}, true);
    options.log = log;

    options.typeTable = options.typeTable || apertureConfig.typeTable || {};
    options.agent = agent;

    return (mahi.createClient(options));
}


function createKeyAPIClient(opts, clients) {
    var log = opts.log.child({component: 'keyapi'}, true);
    var _opts = {
        log: log,
        ufds: opts.ufds
    };

    return (new keyapi(_opts));
}


function onMarlinConnect(clients, marlinClient) {
    clients.marlin = marlinClient;
}


function createMarlinClient(opts, onConnect) {
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

            onConnect(marlinClient);
        }
    });
}


function onMorayConnect(clients, barrier, morayClient) {
    clients.moray = morayClient;
    barrier.done('createMorayClient');
}


function createMorayClient(opts, onConnect) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');

    var log = opts.log.child({component: 'moray'}, true);
    opts.log = log;

    var client = new libmanta.createMorayClient(opts);

    client.once('error', function (err) {
        client.removeAllListeners('connect');

        log.error(err, 'moray: failed to connect');
    });

    client.once('connect', function _onConnect() {
        client.removeAllListeners('error');

        log.info({
            host: opts.host,
            port: opts.port
        }, 'moray: connected');

        onConnect(client);
    });
}

function onRethinkdbConnect(clients, barrier, rethinkdbClient) {
    clients.rethinkdb = rethinkdbClient;
    barrier.done('createRethinkdbClient');
}

function createRethinkdbClient(opts, onConnect) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');

    var log = opts.log.child({component: 'rethinkdb'}, true);
    opts.log = log;

    rethinkdb.connect([], _onConnect);

    client.once('connect', function _onConnect(err, client) {
        log.info({
            host: opts.host,
            port: opts.port
        }, 'rethinkdb: connected');

        onConnect(client);
    });
}

function onCockroachdbConnect(clients, barrier, cockroachdbClient) {
    clients.cockroachdb = cockroachdbClient;
    barrier.done('createCockroachdbClient');
}

function createCockroachdbClient(opts, onConnect) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');

    var log = opts.log.child({component: 'cockroachdb'}, true);
    opts.log = log;

    cockroachdb.connect([], _onConnect);

    client.once('connect', function _onConnect(err, client) {
        log.info({
            host: opts.host,
            port: opts.port
        }, 'cockroachdb: connected');

        onConnect(client);
    });
}


function onMedusaConnect(clients, medusaClient) {
    clients.medusa = medusaClient;
}


function createMedusaConnector(opts, onConnect) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');

    var log = opts.log.child({ component: 'medusa' });
    log.debug({ opts: opts }, 'medusa options');

    var client = medusa.createConnector(opts);

    client.once('connect', function _onConnect() {
        log.info('medusa: connected');
        onConnect(client);
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

    log.info('requisite client connections established, '
    + 'starting muskie servers');

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

    // Parent object for client connection objects.
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

    const opts = parseOptions();
    const cfg = configure(muskie, opts, dtProbes);

    /*
     * Create a barrier to ensure client connections that are established
     * asynchronously and are required for muskie to serve a minimal subset of
     * requests are ready prior to starting up the restify servers.
     */
    var barrier = vasync.barrier();

    barrier.on('drain', clientsConnected.bind(null, muskie, cfg, clients));

    /*
     * Establish minimal set of client connections required to begin
     * successfully servicing non-jobs read requests.
     */

    clients.agent = new cueball.HttpAgent(cfg.cueballHttpAgent);
    clients.mahi = createAuthCacheClient(cfg.auth, clients.agent);

    barrier.start('createMorayClient');
    createMorayClient(cfg.moray, onMorayConnect.bind(null, clients, barrier));

    if (cfg.storage.metadataBackend === 'rethinkdb') {
        barrier.start('createRethinkdbClient');
        createRethinkdbClient(cfg.moray, onRethinkdbConnect.bind(null, clients, barrier));
    } else if (cfg.storage.metadataBackend === 'cockroachdb') {
        barrier.start('createCockroachdbClient');
        createCockroachdbClient(cfg.moray, onCockroachdbConnect.bind(null, clients, barrier));
    }

    barrier.start('createPickerClient');
    createPickerClient(cfg.storage, cfg.log,
        onPickerConnect.bind(null, clients, barrier));

    // Establish other client connections needed for writes and jobs requests.

    // createMarlinClient(cfg.marlin, onMarlinConnect.bind(null, clients));
    // createMedusaConnector(cfg.medusa, onMedusaConnect.bind(null, clients));
    // clients.sharkAgent = createCueballSharkAgent(cfg.sharkConfig);
    clients.keyapi = createKeyAPIClient(cfg);

    // Create monitoring server
    createMonitoringServer(cfg);

    process.on('SIGHUP', process.exit.bind(process, 0));

})();
