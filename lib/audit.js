/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');

var common = require('./common');


///--- Internal Functions

function billable(name, req) {
    assert.string(name, 'name');

    var op;
    switch (name.toLowerCase()) {
    case 'putobject':
    case 'putjobsobject':
    case 'putpublicobject':
    case 'putreportsobject':
    case 'putdirectory':
    case 'putjobsdirectory':
    case 'putpublicdirectory':
    case 'putreportsdirectory':
    case 'postrootdir':
    case 'putlink':
    case 'putjobslink':
    case 'putpubliclink':
    case 'putreportslink':
        op = 'PUT';
        break;

    case 'getrootdir':
    case 'getstorage':
    case 'getjobsstorage':
    case 'getpublicstorage':
    case 'getreportsstorage':
        if (req.metadata && req.metadata.type === 'directory') {
            op = 'LIST';
        } else {
            op = 'GET';
        }
        break;

    case 'deletestorage':
    case 'deletejobsstorage':
    case 'deletepublicstorage':
    case 'deletereportsstorage':
    case 'deleterootdir':
        op = 'DELETE';
        break;

    case 'createjob':
    case 'postjobinputdone':
    case 'postjobinput':
    case 'postjobcancel':
        op = 'POST';
        break;

    case 'getjoberrors':
    case 'getjobfailures':
    case 'getjobinput':
    case 'getjoboutput':
    case 'getjobstatus':
    case 'listjobs':
        op = 'LIST';
        break;

    case 'headstorage':
    case 'headjobsstorage':
    case 'headpublicstorage':
    case 'headreportsstorage':
    case 'headrootdir':
        op = 'HEAD';
        break;

    case 'optionsstorage':
    case 'optionspublicstorage':
    case 'optionsjobsstorage':
    case 'optionsreportsstorage':
        op = 'OPTIONS';
        break;

    default:
        op = undefined;
        break;
    }

    return (op);
}



///--- API

/**
 * Returns a Bunyan audit logger suitable to be used in a server.on('after')
 * event.  I.e.:
 *
 * server.on('after', restify.auditLogger({ log: myAuditStream }));
 *
 * This logs at the INFO level.
 *
 * @param {object} options             - options object.
 * @param {object} options.log         - bunyan logger.
 * @param {object} options.collector   - artedi metric collector.
 * @return {function} to be used with server.on('after').
 */
function auditLogger(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.collector, 'options.collector');

    // Retrieve metric collectors for request count and request latency.
    var request_counter = options.collector.getCollector(
        common.METRIC_REQUEST_COUNTER);
    var deleted_data_counter = options.collector.getCollector(
        common.METRIC_DELETED_DATA_COUNTER);
    var latency_histogram = options.collector.getCollector(
        common.METRIC_LATENCY_HISTOGRAM);
    var time_histogram = options.collector.getCollector(
        common.METRIC_DURATION_HISTOGRAM);

    var log = options.log.child({
        audit: true,
        serializers: {
            err: bunyan.stdSerializers.err,
            req: function auditRequestSerializer(req) {
                if (!req)
                    return (false);

                var caller = {
                    login: null,
                    uuid: null,
                    groups: null,
                    user: null
                };
                var timers = {};
                (req.timers || []).forEach(function (time) {
                    var t = time.time;
                    var _t = Math.floor((1000000 * t[0]) +
                                        (t[1] / 1000));
                    timers[time.name] = _t;
                });

                if (req.caller && req.caller.account) {
                    caller.login = req.caller.account.login || null;
                    caller.uuid = req.caller.account.uuid || null;
                    caller.groups = req.caller.account.groups || null;
                }

                if (req.caller && req.caller.user) {
                    caller.user = {};
                    caller.user.login = req.caller.user.login || null;
                    caller.user.uuid = req.caller.user.uuid || null;
                }

                return ({
                    method: req.method,
                    url: req.url,
                    headers: req.headers,
                    httpVersion: req.httpVersion,
                    version: req.version,
                    body: options.body === true ?
                        req.body : undefined,
                    owner: (req.owner && req.owner.account) ?
                        req.owner.account.uuid : undefined,
                    caller: caller,
                    timers: timers
                });
            },
            res: function auditResponseSerializer(res) {
                if (!res)
                    return (false);

                if (res.req.connection._muskie_error)
                    res.statusCode = 499;

                return ({
                    statusCode: res.statusCode,
                    headers: res._headers,
                    body: options.body === true ?
                        res._body : undefined
                });
            }
        }
    });

    function audit(req, res, route, err) {
        if (req.path() === '/ping')
            return;

        var latency = res.getHeader('X-Response-Time');
        if (typeof (latency) !== 'number')
            latency = Date.now() - req._time;

        var reqHeaderLength = 0;
        Object.keys(req.headers).forEach(function (k) {
            reqHeaderLength +=
            Buffer.byteLength('' + req.headers[k]) +
                Buffer.byteLength(k);
        });

        var resHeaderLength = 0;
        var resHeaders = res.headers();
        Object.keys(resHeaders).forEach(function (k) {
            resHeaderLength +=
            Buffer.byteLength('' + resHeaders[k]) +
                Buffer.byteLength(k);
        });

        var name = route ? (route.name || route) : 'unknown';
        var op = billable(name, req);
        var obj = {
            _audit: true,
            operation: name,
            billable_operation: op,
            bytesTransferred: req._size,
            /*
             * If the request has an X-Forwarded-For header from haproxy,
             * logicalRemoteAddress will be the logical source IP of the client.
             * Otherwise, it will be the same as remoteAddress.
             */
            logicalRemoteAddress: req.connection._xff,
            remoteAddress: req.remoteAddress,
            remotePort: req.connection.remotePort,
            req_id: req.id,
            reqHeaderLength: reqHeaderLength,
            req: req,
            resHeaderLength: resHeaderLength,
            res: res,
            err: err,
            latency: latency,
            secure: req.secure
        };

        /*
         * Muskie keeps a reference to the Moray metadata loaded for the
         * request at req.metadata. In cases where a new object ID is
         * generated (such as a PUT overwriting an existing object,) we
         * want to differentiate between the new object ID for the
         * object and the previous one (and supply both objectIds.)
         */
        if (req.metadata && req.metadata.objectId) {
            if (req.objectId && req.objectId !== req.metadata.objectId) {
                obj.prevObjectId = req.metadata.objectId;
                obj.objectId = req.objectId;
            } else {
                obj.objectId = req.metadata.objectId;
            }
        } else if (req.objectId) {
            /*
             * For a PUT which is creating a fresh object, the req.metadata
             * will have no objectId, we still want to log its objectId, if
             * available.
             */
            obj.objectId = req.objectId;
        }
        obj.sharksContacted = req.sharksContacted;
        obj.entryShard = req.entryShard;
        obj.parentShard = req.parentShard;
        obj.uploadRecordShard = req.uploadRecordShard;
        obj.finalizingRecordShard = req.finalizingRecordShard;

        if (req.route) {
            obj.route = req.route.name;
        }

        if (req._timeToLastByte !== undefined &&
            req._totalBytes !== undefined) {
            obj._auditData = true;
            obj.dataLatency = req._timeToLastByte - req._time;
            obj.dataSize = req._totalBytes;
        }

        if (req._time !== undefined &&
            req._timeAtFirstByte !== undefined) {
            obj.latencyToFirstByte = req._timeAtFirstByte -
                req._time;
        }

        var labels = {
            operation: name,
            method: op || 'unknown',
            statusCode: res.statusCode
        };

        request_counter.increment(labels);
        time_histogram.observe(latency, labels);

        if (obj.latencyToFirstByte) {
            latency_histogram.observe(obj.latencyToFirstByte, labels);
        }

        if (op === 'DELETE' && req.metadata) {
            var md = req.metadata;
            var owner = md.creator || md.owner;

            /*
             * Count bytes for which DELETE API requests have been completed. If
             * the owner of the underlying data has snaplinks disabled, then
             * the deleted object was processed by the accelerated deletion
             * pipeline.
             */
            if (owner && md.type === 'object' && md.contentLength > 0) {
                var storage = md.contentLength * md.sharks.length;
                labels = {
                    accelerated_gc: req.owner.account.snaplinks_disabled,
                    owner: owner
                };
                deleted_data_counter.add(storage, labels);
            }
        }

        log.info(obj, 'handled: %d', res.statusCode);

        return (true);
    }

    return (audit);
}



///-- Exports

module.exports = {
    auditLogger: auditLogger
};
