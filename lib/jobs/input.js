/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var stream = require('stream');
var util = require('util');

var assert = require('assert-plus');
var libmanta = require('libmanta');
var libuuid = require('libuuid');
var lstream = require('lstream');
var once = require('once');
var vasync = require('vasync');

var auth = require('../auth');
var common = require('../common');
var dir = require('../dir');
var jobsCommon = require('./common');
var obj = require('../obj');
require('../errors');



///--- API

function JobInputStream(opts) {
    assert.object(opts, 'options');
    assert.string(opts.jobId, 'options.jobId');
    assert.object(opts.log, 'options.log');
    assert.object(opts.marlin, 'options.marlin');
    assert.optionalNumber(opts.parallel, 'options.parallel');
    assert.optionalNumber(opts.retries, 'options.retries');

    stream.Transform.call(this, opts);
    var self = this;

    this._jobId = opts.jobId;
    this._marlin = opts.marlin;
    var _opts = {
        log: opts.log,
        job: opts.job,
        retry: {
            retries: opts.retries || 1
        }
    };
    this._queue = new libmanta.Queue({
        limit: opts.parallel || 5,
        worker: function addKey(k, cb) {
            self._marlin.jobAddKey(self._jobId, k, _opts, cb);
        }
    });
}
util.inherits(JobInputStream, stream.Transform);


JobInputStream.prototype._transform = function _transform(chunk, enc, cb) {
    if (Buffer.isBuffer(chunk))
        chunk = chunk.toString('utf8');

    cb = once(cb);

    if (!chunk || chunk.length === 0) {
        cb();
    } else if (!this._queue.push(chunk) &&
               !this._queue.listeners('drain').length) {
        this._queue.once('drain', cb);
    } else {
        cb();
    }
};


JobInputStream.prototype._flush = function _flush(cb) {
    this._queue.once('end', cb);
    this._queue.close();
};



// POST handlers

function setup(req, res, next) {
    req.ensureInputNotDone = true;
    req.useJobCache = true;
    next();
}


function submitInputKeys(req, res, next) {
    var job_stream = new JobInputStream({
        encoding: 'utf8',
        jobId: req.job.jobId,
        log: req.log,
        job: req.job,
        marlin: req.marlin
    });
    var line_stream = new lstream({
        encoding: 'utf8'
    });
    var log = req.log;

    line_stream.once('error', next);
    job_stream.once('error', next);
    req.once('error', next);

    job_stream.once('end', function respond() {
        if (!req.params.end) {
            log.debug('submitInputKeys: done (input open)');
            res.send(204);
            return;
        }

        var endoptions = {
            log: log,
            retry: {
                retries: 2
            }
        };
        req.marlin.jobEndInput(req.job.jobId, endoptions, function (err) {
            if (err) {
                next(err);
                return;
            }

            log.debug('submitInputKeys: done (and input ended)');
            res.send(204);
            next();
        });
    });

    res.setTimeout(0);
    req.pipe(line_stream).pipe(job_stream);
    job_stream.resume();
}



// GET Handlers

function getJobInput(req, res, next) {
    req.log.debug({id: req.job.id}, 'getJobInput: entered');

    var keys = [];
    var log = req.log;
    var opts = {
        limit: 100,
        log: log,
        sort_order: 'DESC'
    };
    var query = req.marlin.jobFetchInputs(req.params.id, opts);

    query.once('error', function (err) {
        query.removeAllListeners('end');
        query.removeAllListeners('key');
        next(err);
    });

    query.on('key', function (key) {
        keys.push(key);
    });

    query.once('end', function () {
        query.removeAllListeners('error');
        query.removeAllListeners('key');

        keys.reverse();

        res.header('Content-Type', 'text/plain');
        res.writeHead(200);
        keys.forEach(function (k) {
            res.write(k + '\n');
        });
        res.end();

        next();
    });
}



///--- Exports

module.exports = {
    addInputHandler: function addInputHandler() {
        var chain = [
            setup,
            jobsCommon.loadJob,
            jobsCommon.jobContext,
            auth.authorizationHandler(),
            jobsCommon.ensureJobState,
            submitInputKeys
        ];
        return (chain);
    },

    getInputHandler: function getInputHandler() {
        var chain = [
            jobsCommon.loadJob,
            jobsCommon.jobContext,
            auth.authorizationHandler(),
            getJobInput
        ];
        return (chain);
    }
};
