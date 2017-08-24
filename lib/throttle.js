/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var vasync = require('vasync');
var uuid = require('node-uuid');
var util = require('util');
var mod_url = require('url');
var fs = require('fs');
var dtrace = require('dtrace-provider');
var restify = require('restify');
var Ajv = require('ajv');
var ajv = new Ajv({allErrors: true});

require('./errors');


// Every tunable that is exposed to dynamic configuration
// via the configuration server must be listed here.
var TUNABLES = [
    'requestRateCapacity', // requests per second
    'rateCheckInterval',   // seconds
    'queueTolerance',      // integer number of requests
    'concurrency',         // integer number of slots
    'enabled'              // boolean
];

// Overview of Tunables
//
// requestRateCapacity - the request rate, in units of requests per second,
// above which, after 'queueTolerance' requests are queued up in the pending
// state, the throttle will begin to return HTTP 429s to clients issuing any
// new requests.
//
// rateCheckInterval - the amount of time, in seconds, that the throttle
// should wait before checking the request rate again. The request rate is
// measured only for a given check interval to avoid past measurements from
// skewing the throttle's notion of what load manta is currently under.
//
// queueTolerance - after the requestRateCapacity is reached, the throttle
// will allow up to 'queueTolerance' requests to be put in the pending state
// before returning 429s.
//
// concurrency - the number of slots the request vasync queue has for scheduling
// request-handling worker callbacks concurrently. When all the slots are
// filled, the request queue will starting putting callbacks in the 'pending'
// state.
//
// enabled - if true, the throttle will work as specified in RFD 110. It will
// queue and throttle requests. If false, the throttle will do everything BUT
// queue and throttle requests. This means that clients can still use dtrace
// to see when the throttle _would have_ throttled a request and can also
// view related statistics described in RFD 110 and further comments.

// Defaults
var DEFAULT_ENABLED = false;
var DEFAULT_CONCURRENCY = 50;
var DEFAULT_RATE_CHECK_INTERVAL_S = 3;
var DEFAULT_REQUEST_RATE_CAP = 5000;
var DEFAULT_QUEUE_TOLERANCE = 10;

///--- Exports

module.exports = {

    createThrottle: function createThrottle(options) {
        return (new Throttle(options));
    },

    throttleHandler: function (throttle) {
        function doThrottle(req, res, next) {
            throttle.wait(req, res, next);
        }
        return (doThrottle);
    }

};


// The Throttle object:
//      - Exposes the 'wait' function that should called by invokers
//        in any request-processing codepath to enable throttle
//        functionality.
//      - Runs a configuration server that exposes the fields in the
//        global TUNABLES array to dynamic configuration via HTTP.
//        All the fields that are configurable in this regard are
//        top-level members of the throttle object.
//      - Registers it's own dtrace-provider to expose statistics and
//        other relevant throttle state information.
function Throttle(options) {
    assert.optionalNumber(options.concurrency, 'options.concurrency');
    assert.optionalNumber(options.requestRateCap, 'options.requestRateCap');
    assert.optionalNumber(options.reqRateCheckIntervalSec,
            'options.reqRateCheckIneervalSec');
    assert.optionalNumber(options.queueTolerance, 'options.queueTolerance');
	assert.optionalObject(options.configServer, 'options.configServer');
	assert.optionalObject(options.log, 'options.log');
    assert.optionalBool(options.enabled, 'options.enabled');

    var self = this;

    if (options.log) {
        this.log = options.log;
    } else {
        this.log = bunyan.createLogger({ name: 'throttle' });
    }

    var configServerOpts = {
            port: 10081
    };
    this.validatorCache = {};
    this.configServer = startConfigServer(this, configServerOpts);

    this.dtp = dtrace.createDTraceProvider('muskie-throttle');

    this.throttle_probes = {
        // number of 'pending' requests in the request queue
        request_received: this.dtp.addProbe('request_received', 'int'),
        // most recent observed request rate
        request_rate_checked: this.dtp.addProbe('request_rate_checked', 'int'),
        // latency of the handled request, average request latency
        request_handled: this.dtp.addProbe('request_handled', 'int', 'int'),
        // number of pending requestsm, request rate, url, method
        request_throttled: this.dtp.addProbe('request_throttled', 'int', 'int',
                'char *', 'char *')
    };
    this.dtp.enable();

    var enabled = DEFAULT_ENABLED;
    if (options.enabled) {
        enabled = options.enabled;
    }
    this.enabled = enabled;

    var concurrency = DEFAULT_CONCURRENCY;
    if (options.concurrency && options.concurrency > 0) {
        concurrency = options.concurrency;
    }
    this.concurrency = concurrency;

    var reqRateCap = DEFAULT_REQUEST_RATE_CAP;
    if (options.requestRateCap && options.requestRateCap > 0) {
        reqRateCap = options.requestRateCap;
    }
    this.requestRateCapacity = reqRateCap;

    var queueTolerance = DEFAULT_QUEUE_TOLERANCE;
    if (options.queueTolerance && options.queueTolerance >= 0) {
        queueTolerance = options.queueTolerance;
    }
    this.queueTolerance = queueTolerance;

    var rateCheckInterval = DEFAULT_RATE_CHECK_INTERVAL_S;
    if (options.reqRateCheckIntervalSec &&
            options.reqRateCheckIntervalSec > 0) {
        rateCheckInterval = options.reqRateCheckIntervalSec;
    }
    this.rateCheckInterval = rateCheckInterval;

    this.requestQueue = vasync.queue(function (task, callback) {
        task(callback);
    }, this.concurrency);

    this.lastCheck = Date.now();
    this.mostRecentRequestTime = 0;
    this.mostRecentRequestRate = 0.0;
    this.requestsPerCheckInterval = 0;

    this.averageRequestLatencyMS = 0.0;
    this.requestsHandled = 0;

    this.log.info({
        rateCheckInterval: this.rateCheckInterval,
        requestRateCapacity: this.requestRateCapacity,
        concurrency: this.concurrency
    }, 'created throttle');
}


function startConfigServer(throttle, options) {
    var configServer = restify.createServer({ servername: 'throttle' });

    configServer.get('/getConfig', getConfigHandler.bind(throttle)());
    configServer.post('/updateConfig', updateConfigHandler.bind(throttle)());

    configServer.listen(options.port, '0.0.0.0', function () {
        throttle.log.info('throttle config server started on port %d',
            options.port);
    });

    return (configServer);
};


// Validates the json object stored in req.body with the
// given validator. If req.body is a string, this function
// will first attempt to parse it into a json object.
function validateSchema(validator, req, res, next) {
    if (!req.body || req.body === {}) {
        next();
        return;
    }
    if (typeof(req.body) === 'string') {
        try {
            req.body = JSON.parse(req.body);
        } catch (e) {
            next(new Error(e.toString()));
            return;
        }
    } else if (typeof(req.body) !== 'object') {
        next(new Error('received unsupported type for json validation'));
        return;
    }
    if (validator(req.body)) {
        next();
        return;
    }
    next(new Error(ajv.errorsText()));
}


// Manages the cache of available validators for the ajv
// schema validation engine. Each configuration server endpoint
// has a single schema which is used to compile a validator
// the first time such a request is received.
Throttle.prototype.getValidatorForUrl =
function getValidatorForUrl(url, schema) {
    var validator;
    if (this.validatorCache.hasOwnProperty(url)) {
        validator = this.validatorCache[url];
    } else {
        validator = ajv.compile(schema);
        this.validatorCache[url] = validator;
    }
    return (validator);
}


// Generic function for creating a post request that
// requires modifying throttle state. The returned
// chain takes care of parsing the json body, validating
// it against the schema, and invoking a handler that
// will actually modify the throttle object.
Throttle.prototype.postHandler = function setHandler(url, schema, handler) {
    var validator = this.getValidatorForUrl(url, schema);

    var chain = [
        restify.jsonBodyParser({
            maxParams: false,
            maxBodySize: 500000
        }),
        validateSchema.bind(null, validator),
        handler.bind(this),
    ];

    return (chain);
};


// Generic function for creating a get request handler
// that requires reading throttle state. The retuned
// chain parses get url parameters, and validates the
// corresponding json object that the url npm package
// builds from the query string. In the last step it
// invokes a handler that reads the state and returns
// the requested configuration fields to the client.
Throttle.prototype.getHandler = function getHandler(url, schema, handler) {
    var validator = this.getValidatorForUrl(url, schema);

    function parseAndValidateFields(req, res, next) {
        var fields;
        try {
            fields = mod_url.parse(req.url, true).query;
        } catch (e) {
            next(e);
        }
        req.body = fields;
        next();
    }

    var chain = [
        parseAndValidateFields,
        validateSchema.bind(null, validator),
        handler.bind(this)
    ];

    return (chain);
};


// Returns a handler chain for processing generic
// configuration reads. The client sends the throttle
// server a request that contains a single array
// containing the names of the requested config fields.
// The chain this function returns turns that into a
// json object containing the current values of those
// fields the throttle is operating with.
function getConfigHandler(req, res, next) {
    var url = '/getConfig';
    var schema = {
        'properties': {
            'fields': {
                'type': 'array',
                'items': {
                    'type': 'string',
                },
                'uniqueItems': true
            }
        },
    };

    function getFields (req, res) {
        var self = this;
        var obj = {};
        var fields = req.body.fields || TUNABLES;
        fields.forEach(function (field) {
            if (TUNABLES.indexOf(field) >= 0) {
                obj[field] = self[field];
            }
        });
        res.send(201, obj);
    }

    return this.getHandler(url, schema, getFields);
}


// Returns a handler chain for processing generic
// configuration writes. The client sends a json
// object containing the fields and values that
// it wants the throttle to use. This chain updates
// the state of the throttle so that it begins using
// those config values.
function updateConfigHandler(req, res, next) {
    var url = '/updateConfig';
    var schema = {
        'properties': {
            'rateCheckInterval': {
                'type': 'number',
                'exclusiveMinimum': 0
            },
            'queueTolerance': {
                'type': 'number',
                'minimum': 0
            },
            'requestRateCapacity': {
                'type': 'number',
                'exclusiveMinimum': 0
            },
            'concurrency': {
                'type': 'number',
                'minimum': 1
            },
            'enabled': {
                'type': 'boolean'
            }
        },
        'additionalProperties': false
    };

    function updateFields(req, res, next) {
        var self = this;
        if (!req.body) {
            req.body = {};
        }
        Object.keys(schema.properties).forEach(function (property) {
            if (req.body[property] !== undefined) {
                self[property] = req.body[property];
                if (property === 'concurrency') {
                    self.requestQueue.updateConcurrency(req.body[property]);
                }
            }
        });
        res.send(201);
    }

    return this.postHandler(url, schema, updateFields);
}


// Computes the observed request rate in the most recent
// check interval. This function is called approximately
// every 'rateCheckInterval' seconds. The returned figure
// has unit requests per second.
Throttle.prototype.computeRequestRate = function computeRequestRate() {
    var timeInterval = (this.mostRecentRequestTime - this.lastCheck)/1000;
    if (timeInterval === 0) {
        return (0);
    }
    var requestRate = (this.requestsPerCheckInterval / timeInterval);
    this.lastCheck = this.mostRecentRequestTime;

    return (requestRate);
};


// Upates the running average request latency. Note that
// this figure is reset during every check interval. The
// motivation for this is that we could have had a couple
// of very long-running requests in the last check interval
// that skew the average in later check intervals.
//
// For now this function is only used for statistics
// collection.
Throttle.prototype.updateAverageRequestLatency = function update(newLatency) {
    this.averageRequestLatencyMS *= this.requestsHandled;
    this.averageRequestLatencyMS += newLatency;
    this.requestsHandled++;
    this.averageRequestLatencyMS /= this.requestsHandled;
};


// This is the API method that users of the module
// invoke in a request-processing code path. Roughly,
// The function notes that a new request has arrived,
// checks how long it's been since the last time the
// module checked the request rate and, if that figure
// is above a configurable theshold, it computes the
// request rate, caching it as the most recent request
// rate available.
//
// If at any point, the most recently observed request
// rate is above a configurable capacity and there are
// too many request already queued, the request is
// throttled. If the request is not throttled, a worker
// function is placed on the request queue that issues
// the callback required to handle the request and, as
// an auxiliary operation, computes the request's latency
// defined to be the difference between the time the
// request is queued, and the time that it's handler
// function finishes executing.
Throttle.prototype.wait = function wait(req, res, next) {
    var self = this;

    self.mostRecentRequestTime = Date.now();
    self.requestsPerCheckInterval++;

    var elapsedSec = (self.mostRecentRequestTime - self.lastCheck)/1000;
    if (elapsedSec > self.rateCheckInterval) {
        self.mostRecentRequestRate = self.computeRequestRate();
        self.requestsPerCheckInterval = 0;

        self.throttle_probes.request_rate_checked.fire(function () {
            return ([self.mostRecentRequestRate]);
        });
    }

    self.throttle_probes.request_received.fire(function () {
        return ([self.requestQueue.length()]);
    });

    if ((self.mostRecentRequestRate > self.requestRateCapacity) &&
            (self.requestQueue.length() >= self.queueTolerance)) {
        self.throttle_probes.request_throttled.fire(function () {
            return ([self.requestQueue.length(), self.mostRecentRequestRate,
                req.url, req.method]);
        });
        if (self.enabled) {
            next(new ThrottledError());
            return;
        }
    }

    if (self.enabled) {
        var startTime = Date.now();
        self.requestQueue.push(function (cb) {
            next();
            cb();

            var endTime = Date.now();
            var latency = endTime - startTime;

            self.updateAverageRequestLatency(latency);
            self.throttle_probes.request_handled.fire(function () {
                return ([latency, self.averageRequestLatencyMS]);
            });
        });
    } else {
        var startTime = Date.now();
        next();
        var latency = Date.now() - startTime;
        self.updateAverageRequestLatency(latency);
        self.throttle_probes.request_handled.fire(function () {
            return ([latency, self.averageRequestLatencyMS]);
        });
    }
};
