/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var fs = require('fs');

var artedi = require('artedi');
var assert = require('assert-plus');
var bunyan = require('bunyan');
var jsprim = require('jsprim');
var LRU = require('lru-cache');
var restify = require('restify');

var uploadsCommon = require('./uploads/common');

///--- Constants

const DEF_MAX_STREAMING_SIZE_MB = 51200;
const DEF_MAX_PERCENT_UTIL = 90;
const DEF_MAX_OPERATOR_PERCENT_UTIL = 92;

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
    assert.object(cfg.moray, 'cfg.moray');
    assert.object(cfg.cueballHttpAgent, 'cfg.cueballHttpAgent');
    assert.object(cfg.sharkConfig, 'cfg.sharkConfig');
    assert.optionalObject(cfg.storage, 'cfg.storage');

    /* Used by artedi and RBAC */
    assert.string(cfg.datacenter, 'cfg.datacenter');
    assert.string(cfg.region, 'cfg.region');

    var log = configureLogging(appName, cfg.bunyan, opts.verbose);

    cfg.insecurePort = opts.insecure_port || cfg.insecurePort;
    cfg.port = opts.port || cfg.port;
    cfg.name = 'Manta/2';
    cfg.version = apiVersion();

    if (log.debug()) {
        // Log a "reasonable" set of the app config.
        // - Elide password/key stuff.
        // - Avoid logging `cfg.log` and similar which results in an accidental
        //   huge log record.
        var loggableCfg = jsprim.deepCopy(cfg);
        delete loggableCfg.authToken;
        if (loggableCfg.ufds) {
            delete loggableCfg.ufds.bindPassword;
        }
        log.debug({loggableCfg: loggableCfg}, 'loggable config');
    }

    cfg.log = log;
    [ cfg.auth, cfg.moray,
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
    setNumericConfigProperty('defaultMaxStreamingSizeMB',
        DEF_MAX_STREAMING_SIZE_MB, cfg.storage, cfg.log,
        function (x) { return (x >= 1); });

    if (!cfg.hasOwnProperty('accountsSnaplinksDisabled')) {
        cfg.accountsSnaplinksDisabled = [];
    } else {
        assert.arrayOfObject(cfg.accountsSnaplinksDisabled,
            'cfg.accountsSnaplinksDisabled');
        for (var i = 0; i < cfg.accountsSnaplinksDisabled.length; i++) {
            var uuid = cfg.accountsSnaplinksDisabled[i].uuid;

            assert.uuid(uuid, 'cfg.accountsSnaplinksDisabled[i].uuid');
            cfg.log.info('snaplinks disabled for uuid ' + uuid);
        }
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

    setNumericConfigProperty('maxUtilizationPct',
        DEF_MAX_PERCENT_UTIL, cfg.storage, cfg.log,
        function (x) { return (x > 0 && x <= 100); });
    setNumericConfigProperty('maxOperatorUtilizationPct',
        DEF_MAX_OPERATOR_PERCENT_UTIL, cfg.storage, cfg.log,
        function (x) { return (x > 0 && x <= 100); });

    /*
     * If the configuration is invalid such that the maximum utilization for
     * normal request is greater than the maximum utilization for operator
     * requests then log a warning and use the greater of the default maximum
     * operator utilization or the maximum utilization for normal requests.
     */
    if (cfg.storage.maxUtilizationPct > cfg.storage.maxOperatorUtilizationPct) {
        if (DEF_MAX_OPERATOR_PERCENT_UTIL > cfg.storage.maxUtilizationPct) {
            cfg.log.warn('invalid configuration: "maxUtilizationPct" value (' +
                cfg.storage.maxUtilizationPct + ') should not exceed the ' +
                'value for maxOperatorUtilizationPct (' +
                cfg.storage.maxOperatorUtilizationPct + '). Using the default' +
                ' operator utilization value of ' +
                DEF_MAX_OPERATOR_PERCENT_UTIL + ' as the value for ' +
                'maxOperatorUtilizationPct.');
            cfg.storage.maxOperatorUtilizationPct =
                DEF_MAX_OPERATOR_PERCENT_UTIL;
        } else {
            cfg.log.warn('invalid configuration: "maxUtilizationPct" value (' +
                cfg.storage.maxUtilizationPct + ') should not exceed the ' +
                'value for maxOperatorUtilizationPct (' +
                cfg.storage.maxOperatorUtilizationPct + '). Using ' +
                cfg.storage.maxUtilizationPct + ' as the value for ' +
                'maxOperatorUtilizationPct.');
            cfg.storage.maxOperatorUtilizationPct =
                cfg.storage.maxUtilizationPct;
        }
    }

    cfg.collector = artedi.createCollector({
        labels: {
            datacenter: cfg.datacenter,
            server: cfg.server_uuid,
            zonename: cfg.zone_uuid,
            pid: process.pid
        }
    });

    cfg.dtrace_probes = dtProbes;

    log.debug('config loaded');

    return (cfg);
}


/**
 * Configure the logger based on the configuration data
 *
 * @param {String} appName: Required. The name of the application
 * @param {Object} bunyanCfg: Optional. The bunyan configuration data
 * @param {arrayOfBool} verbose: Optional. Array of boolean values that
 * indicates if verbose logging should be enabled and the level of verbosity
 * requested.
 * @returns {Object} A bunyan logger object
 */
function configureLogging(appName, bunyanCfg, verbose) {
    assert.optionalObject(bunyanCfg, 'config.bunyan');

    var level;
    if (bunyanCfg) {
        assert.optionalString(bunyanCfg.level, 'config.bunyan.level');

        level = bunyan.resolveLevel(process.env.LOG_LEVEL ||
                    bunyanCfg.level || 'info');
    } else {
        level = bunyan.resolveLevel(process.env.LOG_LEVEL || 'info');
    }

    if (verbose) {
        level = Math.max(bunyan.TRACE, (level - verbose.length * 10));
    }

    var streams = [];

    streams.push({
            level: level,
            stream: process.stderr
    });

    if (level >= bunyan.INFO) {
        /*
         * We want debug info IFF the request fails AND the configured
         * log level is info or higher.
         */
        const RequestCaptureStream = restify.bunyan.RequestCaptureStream;
        streams.push({
            level: 'debug',
            type: 'raw',
            stream: new RequestCaptureStream({
                level: bunyan.WARN,
                maxRecords: 2000,
                maxRequestIds: 2000,
                stream: process.stderr
            })
        });
    }

    var log = bunyan.createLogger({
        name: appName,
        streams: streams,
        serializers: restify.bunyan.serializers
    });

    /*
     * If the configured logging level is at or below DEBUG then enable
     * source logging.
     */
    if (level <= bunyan.DEBUG) {
        log = log.child({src: true});
    }

    return (log);
}


/**
 * Verify the type of a numeric configuration property and set a default value
 * if the property is not defined. If the value for the specified configuration
 * property is not of type Number the process exits. Additionally the caller may
 * supply a predicate that is used to evaluate the value given for the property.
 * The process exits if the value does not conform to the predicate if it is
 * supplied.
 *
 * @param {String} property: Required. The name of the configuration property.
 * @param {Number} dfault: Required. A default value.
 * @param {Object} config: Required. The configuration object.
 * @param {Object} log: Required. A logging object.
 * @param {Function} predicate: Optional. A function from Number to Boolean.
 */
function setNumericConfigProperty(property, dfault, config, log, predicate) {
    assert.string(property, 'property');
    assert.number(dfault, 'default property value');
    assert.object(config, 'config');
    assert.object(log, 'log');
    assert.optionalFunc(predicate, 'predicate');

    if (config.hasOwnProperty(property)) {
        var cfgVal = config[property];

        /*
         * Ensure the configuration value for the property is of type
         * Number. Also apply a property-specific predicate to the value if
         * provided by the caller. If the value does not conform then the
         * process exits.
         */
        if (typeof (cfgVal) !== 'number') {
            log.fatal('invalid "' + cfgVal + '" value');
            process.exit(1);
        } else if (predicate && !predicate(cfgVal)) {
            log.fatal('invalid "' + cfgVal + '" value');
            process.exit(1);
        }
    } else {
        config[property] = dfault;
    }
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


function apiVersion() {
    //
    // This used to use the package.json version and clients are depending on
    // ~1.0. Since Manta wanted to go to v2, we need to hardcode this so that
    // old clients still work even as functionality is deprecated in Manta v2.
    // The functionality that *does* still exist, is still compatible with the
    // old tools and is likely to remain so. If not, this will need to be bumped
    // too and all clients updated.
    //
    return ('1.0.0');
}


module.exports = {
    configure: configure,
    configureLogging: configureLogging
};
