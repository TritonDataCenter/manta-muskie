/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');
var vasync = require('vasync');
var libuuid = require('libuuid');
var once = require('once');
var util = require('util');
var mod_url = require('url');
var fs = require('fs');
var dtrace = require('dtrace-provider');
var restify = require('restify');
var jsprim = require('jsprim');
var VError = require('verror');

require('./errors');

/*
 * Request Throttling
 *
 * This module implements a coarse request throttle with a vasync worker
 * queue. Requests enter the queue in throttlePreHandler, and are removed
 * throttleAfterHandler. Between the time that a request is pushed into
 * the queue and the time that its vasync callback is invoked, it will
 * go through two stages.
 *
 * (1) A recently added request will be queued for dispatch. This means
 *     the remainder of its restify handler chain is paused.
 * (2) If the requestQueue has an open slot, the request will be dispatched,
 *     resuming the remainder of its restify handler chain.
 *
 * The vasync work queue itself accepts a 'concurrency' parameter. This
 * determines the number of requests that can be in stage (2). The throttle
 * has an additional parameter: 'queueTolerance'. This determines the
 * number of requests which are allowed to be in stage (1). While 'concurrency'
 * is enforced by the vasync work queue logic, the 'queueTolerance' is enforced
 * by the logic in this module.
 *
 * Requests enter stage (1) because the throttlePreHandler is installed as
 * a 'use' handler via the restify server API for each incoming request.
 * Request enter stage (2) once the vasync queue finds a slot. A request leaves
 * stage (2) in one of two ways:
 *
 * (1) The server emits either an 'after' or 'uncaughtException' event for
 *     the request. Since the throttleAfterHandler is installed as a listener
 *     for both of these events, the request's queue slot will be cleared once
 *     its vasync callback is invoked.
 *
 * (2) Muskie sends a response with res.send API without invoking the restify
 *     `next` function in the last handler of the route and the request lies
 *     dormant in the queue for a configurable time interval. At this point
 *     the `_reapStaleRequests` method will find that a response has been
 *     sent out for the request and invoke its vasync callback. The
 *     'reapInterval' configuration parameter controls how frequently Muskie
 *     checks for stale entries in the request queue.
 *
 * Throttle Parameter Tradeoffs
 *
 * Under load, a Muskie throttle with high concurrency and low queueTolerance
 * will allow Muskie to process more requests at once while maintaining a lower
 * memory footprint. Such a configuration should be used when CPU resources
 * are not limited.
 *
 * Under load, a Muskie throttle with low concurrency and high queue tolerance
 * will limit the number of concurrency requests and potentially lead to longer
 * queuing delays. Such a configuration can result in a lower Muskie CPU
 * utilization at the cost of request latency.
 *
 * Throttling is disabled by default.
 */

/*
 * Specifies how frequently the throttle should audit its map of all in-flight
 * requests for requests that have been responded to but remain in the queue
 * because of a missing call to `next` in a restify handler. See the comment
 * above Throttle#_reapStaleRequests for more information. The larger this
 * value is, the more likely the we are to build up requests that have had
 * responses sent but are taking up space in the queue. Smaller values may
 * result in more frequent unnecessary checks which will increase the CPU
 * footprint of the Muskie process.
 *
 * Since _reapStaleRequests is trying to compensate for programmer error in
 * Muskie, there is no correct value here. In experiments, 5 seconds was shown
 * to catch at most 1 stale request per iteration.
 */
var DEFAULT_REAP_INTERVAL_MS = 5000;

/*
 * Constructor for the throttle which kicks off periodic request reaping.
 */
function Throttle(options) {
    assert.number(options.concurrency, 'options.concurrency');
    assert.ok(options.concurrency > 0, 'concurrency must be positive');
    assert.bool(options.enabled, 'options.enabled');
    assert.ok(options.log, 'options.log');
    assert.number(options.queueTolerance, 'options.queueTolerance');
    assert.ok(options.queueTolerance > 0, 'queueTolerance must be positive');
    assert.object(options.server, 'restify server');

    this.log = options.log.child({
        component: 'Throttle'
    });

    this.server = options.server;
    this.requestMap = {};

    this.dtp = dtrace.createDTraceProvider('Muskie-throttle');
    this.throttle_probes = {
        // request id
        request_throttled: this.dtp.addProbe('request_throttled', 'char *'),
        // request id
        request_handled: this.dtp.addProbe('request_handled', 'char *'),
        // request id
        request_reaped: this.dtp.addProbe('request_reaped', 'char *'),
        // request id
        queue_enter: this.dtp.addProbe('queue_enter', 'char *'),
        // request id
        queue_leave: this.dtp.addProbe('queue_leave', 'char *'),
        // num queued, num in-flight
        throttle_stats: this.dtp.addProbe('throttle_stats', 'int', 'int')
    };
    this.dtp.enable();

    this.enabled = options.enabled;
    this.concurrency = options.concurrency;
    this.queueTolerance = options.queueTolerance;

    /*
     * 'task' refers to a chain of restify handlers. 'callback' is a hook back
     * into the vasync code that will be called once the request has been
     * processed.
     */
    this.requestQueue = vasync.queue(function (task, callback) {
        task(callback);
    }, this.concurrency);

    this.reapInterval = options.reapInterval || DEFAULT_REAP_INTERVAL_MS;
    this._reapStaleRequests();
}

/*
 * In the unfortunate event that a restify handler chain contains a handler that
 * does not always properly invoke the restify `next` callback, the restify
 * server will emit neither an `after` event nor an `uncaughtException`
 * event for the request that was being serviced. Since the throttle listens for
 * these events to determine when request handling is complete, such missing
 * `next` calls can lead to leaked requests in the throttle.
 *
 * To guard against resource leaks caused by past and future programmer errors
 * of this type, we periodically scan the throttle's request map and invoke the
 * vasync callback for requests for which Muskie has already sent a response.
 * Invoking this callback frees up a vasync queue slot, ensuring that these
 * slots don't become occupied indefinitely by stale requests.
 *
 * This function schedules itself to be called every 'Throttle#reapInterval'
 * milliseconds. It will do so for the lifetime of a Muskie process so long as
 * the throttle is enabled.
 */
Throttle.prototype._reapStaleRequests = function reap() {

    function hrtimeToMS(hrtime) {
        return (hrtime[0]*1e3) + (hrtime[0]/1e6);
    }

    var self = this;

    self.log.debug({
        numReqs: Object.keys(self.requestMap).length
    }, 'checking for stale requests');

    Object.keys(self.requestMap).forEach(function (key) {
        var value = self.requestMap[key];

        var req = value.req;
        var res = value.res;
        var queueCb = value.queueCb;

        assert.object(req, 'req');
        assert.object(req, 'res');
        assert.func(queueCb, 'queueCb');

        /*
         * Flag set by node core when res.end() is invoked by restify.
         */
        if (res.finished) {
            self.log.debug({
                reqId: req.getId()
            }, 'reaping stale request slot');

            self.throttle_probes.request_reaped.fire(function () {
                return ([req.getId()]);
            });

            delete (self.requestMap[key]);

            queueCb();
        }
    });

    setTimeout(self._reapStaleRequests.bind(self), self.reapInterval);
};


/*
 * Returns a restify handler function which pauses incoming request
 * routes or throttles them if there are 'concurrency' requests already
 * dispatched and 'queueTolerance' requests already waiting for those
 * slots.
 */
Throttle.prototype.throttlePreHandler = function preHandler() {
    var self = this;

    function throttleWait(req, res, next) {
        self.log.debug({
            reqId: req.getId()
        }, 'throttlePreHandler: enter');

        req.throttle = {
            reqId: libuuid.create()
        };

        if (self.requestQueue.length() >= self.queueTolerance) {
            self.throttle_probes.request_throttled.fire(function () {
                return ([req.getId()]);
            });

            next(new VError(new ThrottledError(), 'request %s throttled ' +
                '(queued = %d, in-flight = %d)', req.getId(),
                self.requestQueue.length(), self.requestQueue.npending));
            return;
        }

        assert.ok(Object.keys(self.requestMap).length <= self.concurrency,
                'fewer than concurrency callbacks');

        self.throttle_probes.queue_enter.fire(function () {
            return ([req.getId()]);
        });

        function startRoute(cb) {
            assert.ok(!self.requestMap[req.throttle.reqId],
                'attempting to register throttle handler twice');

            /*
             * The callback 'cb' is wrapped in a 'once' because once a request
             * enters the map, there are two ways it can leave:
             *  (1) it is reaped from the request map
             *  (2) it is deleted from the request map in a callback for
             *  the restify server's 'after' or 'uncaughtException' event
             */
            self.requestMap[req.throttle.reqId] = {
                req: req,
                res: res,
                queueCb: once(cb)
            };

            self.log.debug({
                reqId: req.getId(),
                numreqs: Object.keys(self.requestMap).length
            }, 'throttle request map insert');

            self.throttle_probes.queue_leave.fire(function () {
                return ([req.getId()]);
            });
            next();
        }

        self.requestQueue.push(startRoute);

        self.throttle_probes.throttle_stats.fire(function () {
            return ([self.requestQueue.length(), self.requestQueue.npending]);
        });

        self.log.debug({
            reqId: req.getId()
        }, 'throttlePreHandler: done');
    }

    return (throttleWait);
};

/*
 * Returns a function to be called when either the 'after' or
 * 'uncaughException' event is emitted by the restify server. This
 * function is responsible for cleaning up all metadata the throttle
 * stores for a given request.
 */
Throttle.prototype.throttleAfterHandler = function afterHandler() {
    var self = this;

    function throttleClearQueueSlot(req, _) {
        self.log.debug({
            reqId: req.getId()
        }, 'throttleAfterHandler: enter');

        assert.object(req.throttle, 'req.throttle');
        assert.string(req.throttle.reqId, 'req.throttle.reqId');

        var value = self.requestMap[req.throttle.reqId];
        if (!value) {
            self.log.debug({
                reqId: req.getId()
            }, 'missing request map entry, assuming reaped');
            return;
        }

        assert.object(value.req, 'value.req');
        assert.object(value.res, 'value.res');
        assert.func(value.queueCb, 'value.cb');

        self.log.debug({
            reqId: req.getId(),
            numreqs: Object.keys(self.requestMap).length
        }, 'throttle request map remove');

        delete (self.requestMap[req.throttle.reqId]);

        self.throttle_probes.request_handled.fire(function () {
            return ([req.getId()]);
        });

        value.queueCb();

        self.throttle_probes.throttle_stats.fire(function () {
            return ([self.requestQueue.length(), self.requestQueue.npending]);
        });

        self.log.debug({
            reqId: req.getId()
        }, 'throttleAfterHandler: done');
    }

    return (throttleClearQueueSlot);
};


///--- Exports

module.exports = {

    createThrottle: function createThrottle(options) {
        return (new Throttle(options));
    }

};
