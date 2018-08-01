/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var net = require('net');
var os = require('os');
var path = require('path');

var apertureConfig = require('aperture-config').config;
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
var mahi = require('mahi');
var marlin = require('marlin');
var medusa = require('./lib/medusa');
var once = require('once');
var restify = require('restify');
var vasync = require('vasync');

var app = require('./lib');


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


function onPickerConnect(clients, pickerClient) {
    clients.picker = pickerClient;
}


function createPickerClient(cfg, log, onConnect) {
    var opts = {
        interval: cfg.interval,
        lag: cfg.lag,
        moray: cfg.moray,
        log: log.child({component: 'picker'}, true),
        multiDC: cfg.multiDC,
        defaultMaxStreamingSizeMB: cfg.defaultMaxStreamingSizeMB,
        maxUtilizationPct: cfg.maxUtilizationPct,
        maxOperatorUtilizationPct: cfg.maxOperatorUtilizationPct
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
    const cfg = app.configure(muskie, opts, dtProbes);

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

    // Establish other client connections needed for writes and jobs requests.
    createPickerClient(cfg.storage, cfg.log,
        onPickerConnect.bind(null, clients));
    createMarlinClient(cfg.marlin, onMarlinConnect.bind(null, clients));
    createMedusaConnector(cfg.medusa, onMedusaConnect.bind(null, clients));
    clients.sharkAgent = createCueballSharkAgent(cfg.sharkConfig);
    clients.keyapi = createKeyAPIClient(cfg);

    // Create monitoring server
    createMonitoringServer(cfg);

    process.on('SIGHUP', process.exit.bind(process, 0));

})();
