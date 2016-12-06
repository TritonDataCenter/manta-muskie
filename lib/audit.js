/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');



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
 * @param {Object} options at least a bunyan logger (log).
 * @return {Function} to be used in server.after.
 */
function auditLogger(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

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
            remoteAddress: req.connection.remoteAddress,
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

        if (req.metadata && req.metadata.objectId) {
            obj.objectId = req.metadata.objectId;
        }
        obj.sharksContacted = req.sharksContacted;
        obj.entryShard = req.entryShard;
        obj.parentShard = req.parentShard;

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

        log.info(obj, 'handled: %d', res.statusCode);

        return (true);
    }

    return (audit);
}



///-- Exports

module.exports = {
    auditLogger: auditLogger
};
