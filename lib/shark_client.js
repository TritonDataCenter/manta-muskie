/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var EventEmitter = require('events').EventEmitter;
var http = require('http');
var util = require('util');

var assert = require('assert-plus');
var backoff = require('backoff');
var KeepAliveAgent = require('keep-alive-agent');
var once = require('once');

var common = require('./common');



///--- Globals

var CLIENT_MAP = {};
var MAX_SOCKETS = 8192;



///--- Errors

function ConnectTimeoutError(host, time) {
    Error.captureStackTrace(this, ConnectTimeoutError);
    this.name = 'ConnectTimeoutError';
    this.message = util.format('failed to connect to %s in %dms', host, time);
}
util.inherits(ConnectTimeoutError, Error);


function SharkResponseError(res, body) {
    Error.captureStackTrace(this, SharkResponseError);
    this.name = 'SharkResponseError';
    this.message = util.format('mako failure:\nHTTP %d\n%s%s',
                               res.statusCode,
                               JSON.stringify(res.headers, null, 2),
                               body ? '\n' + body : '');
    this._result = res;
}
util.inherits(SharkResponseError, Error);



///--- Helpers

function _request(opts, cb) {
    cb = once(cb);
    var req = http.request(opts);

    if (opts.body) {
        req.write(JSON.stringify(opts.body));
    }
    /*
     * This timer represents the timeout for connecting to the shark
     * for this request, so it is important that it is cleared only once
     * we have heard some sort of response from the shark.
     *
     * It would be tempting to clear this timer on the 'socket' event, but
     * in the case of reused sockets, we may get a socket whose shark has since
     * disappeared (e.g., behind a network partition). We really want this
     * timeout to cover the interval up to when we know the shark has started
     * processing the request. If it expires before this happens, the caller
     * can retry another shark. If we cleared the timer on the 'socket' event,
     * we would be unable to detect the shark is down until the socket times
     * out (usually after 2 minutes), at which point, it's likely too late to
     * retry the request on a different shark.
     *
     * For GET/HEAD requests, we know it's safe to clear the timer when the
     * response is received.
     * For PUT requests, it can be cleared after a 100-continue is received.
     *
     */
    var connectionTimer = setTimeout(function onTimeout() {
        if (req) {
            req.abort();
        }

        cb(new ConnectTimeoutError(opts.hostname, opts.connectTimeout));
    }, opts.connectTimeout);

    if (opts.method === 'POST') {
        clearTimeout(connectionTimer);
    }

    req.once('error', function onRequestError(err) {
        clearTimeout(connectionTimer);
        cb(err);
    });

    function onResponse(res) {
        clearTimeout(connectionTimer);
        if (res.statusCode >= 400) {
            var body = '';
            res.setEncoding('utf8');
            res.on('data', function (chunk) {
                body += chunk;
            });
            res.once('end', function () {
                cb(new SharkResponseError(res, body), req, res);
            });
            res.once('error', function (err) {
                cb(new SharkResponseError(res, err.toString()), req, res);
            });
            res.resume();
        } else {
            cb(null, req, res);
        }
    }

    req.once('continue', function () {
        clearTimeout(connectionTimer);
        req.removeListener('response', onResponse);
        cb(null, req);
    });

    req.once('response', onResponse);
    if (opts.method !== 'PUT') {
        req.end();
    }
}


function request(thisp, method, opts, cb) {
    cb = once(cb);

    var log = thisp.log;

    var headers = {
        connection: 'keep-alive',
        'x-request-id': opts.requestId
    };

    if (opts.range !== undefined) {
        headers.range = opts.range;
    }

    var _opts = {
        connectTimeout: thisp.connectTimeout,
        headers: opts.headers || headers,
        hostname: thisp.hostname,
        method: method,
        port: thisp.port
    };

    if (!opts.path) {
        _opts.path = '/' + (opts.creator || opts.owner) + '/' + opts.objectId;
    } else {
        _opts.path = opts.path;
    }

    if (opts.body) {
        _opts.body = opts.body;
    }

    log.debug(_opts, 'request: entered');

    // don't log this
    _opts.agent = thisp.agent;

    var retry = backoff.call(_request, _opts, function (err, req, res) {
        if (err) {
            cb(err);
        } else {
            log.debug({
                requestId: opts.requestId
            }, 'request: done');
            cb(null, req, res);
        }
    });
    retry.setStrategy(new backoff.ExponentialStrategy({
        initialDelay: 100,
        maxDelay: 10000
    }));
    retry.failAfter(2);
    retry.start();
}


function sharkToUrl(shark) {
    assert.object(shark, 'shark');
    assert.string(shark.manta_storage_id);

    return ('http://' + shark.manta_storage_id);
}



///--- API

function SharkClient(options) {
    assert.object(options, 'options');
    assert.optionalNumber(options.connectTimeout, 'options.connectTimeout');
    assert.object(options.log, 'options.log');
    assert.optionalObject(options.retry, 'options.retry');
    assert.object(options.shark, 'options.shark');
    assert.object(options.agent, 'options.agent');

    EventEmitter.call(this);

    var self = this;

    this.agent = options.agent;
    this.connectTimeout = options.connectTimeout || 2000;
    this.hostname = options.shark.manta_storage_id;
    this.log = options.log.child({
        component: 'SharkClient',
        mako_hostname: self.hostname
    }, true);
    this.port = 80;

    this.close = once(function close() {
        /*
         * Cueball Agents have a .stop() method. If we find one on our agent,
         * use it.
         */
        if (typeof (self.agent.stop) === 'function') {
            self.agent.stop();
            return;
        }

        /* Otherwise, assume it's a KeepAliveAgent. */
        var sockets = self.agent.idleSockets || {};
        Object.keys(sockets).forEach(function (k) {
            sockets[k].forEach(function (s) {
                s.end();
            });
        });

        sockets = self.agent.sockets || {};
        Object.keys(sockets).forEach(function (k) {
            sockets[k].forEach(function (s) {
                s.end();
            });
        });
    });
}
util.inherits(SharkClient, EventEmitter);



/**
 * Wraps node's http request.
 *
 * Options needs:
 *   - objectId
 *   - owner
 *   - requestId
 *   - range (optional)
 *
 * @param {object} options see above
 * @param {function} callback => f(err, req)
 */
SharkClient.prototype.get = function get(opts, cb) {
    assert.object(opts, 'options');
    assert.string(opts.objectId, 'options.objectId');
    assert.string(opts.owner, 'options.owner');
    assert.string(opts.requestId, 'options.requestId');
    assert.optionalString(opts.range, 'options.range');
    assert.func(cb, 'callback');

    request(this, 'GET', opts, cb);
};


/**
 * Wraps node's http request.
 *
 * Options needs:
 *   - objectId
 *   - owner
 *   - requestId
 *   - range (optional)
 *
 * @param {object} options see above
 * @param {function} callback => f(err, req)
 */
SharkClient.prototype.head = function head(opts, cb) {
    assert.object(opts, 'options');
    assert.string(opts.objectId, 'options.objectId');
    assert.string(opts.owner, 'options.owner');
    assert.string(opts.requestId, 'options.requestId');
    assert.optionalString(opts.range, 'options.range');
    assert.func(cb, 'callback');

    request(this, 'HEAD', opts, cb);
};


/*
 * Wraps up node's http request.
 *
 * Options needs:
 *   - contentLength
 *   - contentType
 *   - objectId
 *   - owner
 *   - requestId
 *
 * @param {object} options see above
 * @param {function} callback => f(err, req)
 */
SharkClient.prototype.put = function put(opts, cb) {
    assert.object(opts, 'options');
    assert.optionalNumber(opts.contentLength, 'options.contentLength');
    assert.string(opts.contentType, 'options.contentType');
    assert.string(opts.objectId, 'options.objectId');
    assert.string(opts.owner, 'options.owner');
    assert.string(opts.requestId, 'options.requestId');
    assert.optionalString(opts.contentMd5, 'options.contentMd5');
    assert.func(cb, 'callback');

    var _opts = {
        headers: {
            connection: 'keep-alive',
            'content-type': opts.contentType,
            expect: '100-continue',
            'x-request-id': opts.requestId
        },
        owner: opts.owner,
        objectId: opts.objectId
    };

    if (opts.contentLength !== undefined) {
        _opts.headers['content-length'] = opts.contentLength;
    } else {
        _opts.headers['transfer-encoding'] = 'chunked';
    }

    if (opts.contentMd5 !== undefined)
        _opts.headers['content-md5'] = opts.contentMd5;

    request(this, 'PUT', _opts, cb);
};


/*
 * Wraps up node's http request.
 *
 * Options needs:
 *   - objectId
 *   - contentType
 *   - objectId
 *   - owner
 *   - requestId
 *
 * @param {object} options see above
 * @param {body} JSON blob to send in POST request
 * @param {function} callback => f(err, req)
 */
SharkClient.prototype.post = function post(opts, body, cb) {
    assert.object(opts, 'options');
    assert.object(body, 'body');
    assert.string(opts.objectId, 'options.objectId');
    assert.string(opts.owner, 'options.owner');
    assert.string(opts.requestId, 'options.requestId');
    assert.func(cb, 'callback');

    opts.body = body;

    request(this, 'POST', opts, cb);
};


SharkClient.prototype.toString = function toString() {
    return ('[object SharkClient<' + this.hostname + '>]');
};


///--- Exports

module.exports = {

    /*
     * Maintains a cache of clients so we're not blowing through ephemeral
     * TCP ports.
     *
     * @params {object} option (see SharkClient)
     * @return {SharkClient} either from cache or created.
     */
    getClient: function getSharkClient(options) {
        assert.object(options, 'options');
        assert.object(options.shark, 'options.shark');

        var client;
        var id = options.shark.manta_storage_id;

        if (!(client = CLIENT_MAP[id])) {
            client = new SharkClient(options);
            CLIENT_MAP[id] = client;
        }

        /*
         * Since we only select a shark client based on the hostname, assert
         * that some important options specified by the caller are the same
         * for this client.
         */
        assert.equal(client.connectTimeout, options.connectTimeout);
        assert.equal(client.hostname, options.shark.manta_storage_id);
        assert.equal(client.agent, options.agent);

        return (client);
    }
};
