/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

// vim: set ts=8 sts=8 sw=8 et:

var util = require('util');
var http = require('http');
var Watershed = require('watershed').Watershed;
var assert = require('assert-plus');
var once = require('once');
var domain = require('domain');
var jobs_common = require('../jobs/common');
var errors = require('../errors');
var auth = require('../auth');

///--- Globals

var WATERSHED = new Watershed();




///--- Internal functions


function validateMedusaParams(req, res, next) {
    var log = req.log;

    log.debug('validateMedusaParams');

    // Ensure that this is an attempt at an Upgrade:
    if (!res.claimUpgrade) {
        log.debug('client must upgrade, but did not');
        next(new errors.ExpectedUpgradeError(req));
        return;
    }

    // Ensure that we are being asked for a meaningful session type:
    if (req.params.type !== 'master' && req.params.type !== 'slave') {
        log.error({
            type: req.params.type
        }, 'type must be master or slave');
        next(new errors.InvalidParameterError(req.params.type, 'type'));
        return;
    }

    next();
}

function findMedusaBackendForSession(req, res, next) {
    var log = req.log;
    var job = req.job;
    var medusa = req.medusa;

    var fallbackToDefault = (req.params.type === 'master');

    log.debug({ job: job }, 'findMedusaBackendForSession');

    var cb = function (err, medusaBackend) {
        if (!err)
            req.medusaBackend = medusaBackend;
        next(err);
    };

    medusa.findMedusaBackend(req.getId(), job.jobId, fallbackToDefault,
                             cb);
}

function connectMedusaBackend(req, res, next) {
    var log = req.log;
    var job = req.job;
    var medusa = req.medusa;
    var backend = req.medusaBackend;

    log.debug({ job: job }, 'connectMedusaBackend');

    var cb = function (err, socket) {
        if (!err)
            req.medusaSocket = socket;
        next(err);
    };

    medusa.createBackendConnection(req.getId(), job.jobId,
                                   req.params.type, backend, cb);
}

function connectMedusaToClient(req, res, next) {
    var log = req.log;
    var job = req.job;

    var upsock, resUpgrade;
    var d;

    try {
        resUpgrade = res.claimUpgrade();

        // Use Watershed for the websockets handshake, but request
        // a detached socket/head pair.  We will treat this as a
        // straight TCP stream from now on.
        upsock = WATERSHED.accept(req, resUpgrade.socket,
                                  resUpgrade.head, true);
    } catch (ex) {
        // Abort the websockets connection to the backend as we're
        // giving up:
        req.medusaSocket.destroy();
        delete req.medusaSocket;

        log.error({ err: ex }, 'error accepting websocket');

        next(new InternalError(ex));
        return;
    }

    d = domain.create();
    d.on('error', function cpOnError(err) {
        log.error(err, 'uncaught error from medusa connection pair');
        throw (err);
    });
    d.run(function cpRun() {
        runConnectionPair({
            log: req.medusa.log,
            requestId: req.getId(),
            jobId: job.jobId,
            type: req.params.type,
            back: {
                socket: req.medusaSocket
            },
            front: {
                socket: upsock
            },
            inactivityTimeout: req.medusa.inactivityTimeout
        });
    });

    delete req.medusaSocket;

    next(false);
}

function runConnectionPair(ctx) {
    ctx.startTime = Date.now();

    var log = ctx.log.child({
        method: 'runConnectionPair',
        type: ctx.type,
        requestId: ctx.requestId,
        jobId: ctx.jobId
    });

    ctx.endDone = false;
    var checkEnd = function cpCheckEnd(had_error) {
        if (ctx.endDone)
            return;
        if (!ctx.back.socket && !ctx.front.socket) {
            var dur = Date.now() - ctx.startTime;
            log.info({ durationMilliseconds: dur },
                     'connection pair ended');
            ctx.endDone = true;
        }
    };

    // We wish to disable Nagle's on these sockets, as they will contain
    // mostly interactive traffic with small packets:
    ctx.back.socket.setNoDelay(true);
    ctx.front.socket.setNoDelay(true);

    // Wait for them to finish.  Also, set up a short inactivity
    // timeout.  Medusa sessions must periodically ping to keep
    // themselves alive.
    var cleanupListeners = function cpCleanupListeners(endName, socket) {
        assert.string(endName, 'endName');
        assert.stream(socket, 'socket');

        socket.on('error', function cpSocketError(err) {
            log.debug({ err: err, end: endName },
                      'connection error for ' + endName);
        });
        socket.on('close', function cpSocketClose(had_error) {
            log.debug({ end: endName, had_error: had_error },
                      'socket closed for ' + endName);
            socket.removeAllListeners();
            ctx[endName].socket = null;
            checkEnd(had_error);
        });
        socket.on('end', function cpSocketEnd() {
            // pipe() will take care of calling end() on
            // the paired stream.
            log.debug({ end: endName },
                      'connection ended by ' + endName);
        });

        socket.removeAllListeners('timeout');
        socket.setTimeout(ctx.inactivityTimeout, function cpTimeout() {
            log.debug({ end: endName },
                      'connection timed out, ending.');
            socket.destroy();
        });
    };
    cleanupListeners('front', ctx.front.socket);
    cleanupListeners('back', ctx.back.socket);

    // Now, link up the two sockets:
    ctx.front.socket.pipe(ctx.back.socket);
    ctx.back.socket.pipe(ctx.front.socket);

    log.debug('connection pair running');
}


///--- Exports

module.exports = {

    getMedusaAttachHandler: function _getMedusaAttach() {
        var chain = [
            //
            // Ensure we have a valid, active Job that we have
            // rights to.
            jobs_common.loadJob,
            jobs_common.jobContext,
            auth.authorizationHandler(),
            jobs_common.ensureJobState,
            //
            // Ensure the client has made a valid Medusa
            // request:
            validateMedusaParams,
            //
            // Find the Medusa Reflector responsible for this
            // session, or randomly select one for new master-end
            // connections:
            findMedusaBackendForSession,
            //
            // Connect to the backend Medusa we selected,
            // and start proxying data from the client:
            connectMedusaBackend,
            connectMedusaToClient
        ];
        return (chain);
    }

};
