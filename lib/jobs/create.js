/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var util = require('util');

var restify = require('restify');
var libuuid = require('libuuid');
var vasync = require('vasync');

var auth = require('../auth');
var common = require('../common');
var jobsCommon = require('./common');
require('../errors');



///--- Globals

var sprintf = util.format;



///--- API

function resumeStream(req, res, next) {
    req.log.debug('resumeStream: passing on');
    process.nextTick(req.resume.bind(req));
    next();
}


function setupJob(req, res, next) {
    var job = req.job = (req.body || {});
    var jobErr;
    var log = req.log;
    var isOperator = req.caller.user ? false : req.caller.account.isOperator;

    if (job.name === undefined)
        job.name = '';

    req.log.debug({
        job: job
    }, 'setupJob: entered');

    if ((jobErr = req.marlin.jobValidate(job, isOperator))) {
        log.debug(jobErr, 'validateJob: input error');
        next(new InvalidJobError(jobErr.message));
    } else {
        job.phases = (job.phases || []).map(function (p) {
            if (!p.type)
                p.type = 'map';

            return (p);
        });

        req.job = job;
        req.job.jobId = libuuid.create();
        req.job.owner = req.owner.account.uuid;

        var aes = req.config.authToken;
        var caller = req.caller;
        var context = req.authContext;
        var principal = {
            roles: caller.roles || {}
        };

        principal.account = {
            uuid: caller.account.uuid,
            login: caller.account.login,
            approved_for_provisioning:
                caller.account.approved_for_provisioning,
            groups: caller.account.groups,
            isOperator: caller.account.isOperator
        };

        if (caller.user) {
            principal.user = {
                uuid: caller.user.uuid,
                account: caller.user.account,
                login: caller.user.login,
                roles: caller.user.roles,
                defaultRoles: caller.user.defaultRoles
            };
        }

        var opts = {
            caller: caller,
            context: context,
            fromjob: true
        };

        auth.createAuthToken(opts, aes, function (err, token) {
            if (err) {
                log.error(err, 'unable to create auth token');
                next(new InternalError());
                return;
            }

            req.job.auth = {
                principal: principal,
                conditions: req.authContext.conditions,

                // legacy job token fields
                login: caller.account.login,
                groups: caller.account.groups,
                uuid: caller.account.uuid
            };
            req.job.auth.token = token;
            req.job.authToken = token;

            log.debug('setupJob: done');
            next();
        });
    }
}


function ensurePutAccess(req, res, next) {
    var log = req.log;

    log.debug('ensurePutAccess: entered');

    if (!req.caller.user) {
        next();
        return;
    }

    /*
     * Create an artificial resource with role tags matching the caller's active
     * roles.
     */
    var resource = {
        roles: req.activeRoles,
        owner: req.owner
    };

    var opts = {
        principal: req.authContext.principal,
        action: 'putobject',
        resource: resource,
        conditions: req.authContext.conditions
    };

    try {
        req.mahi.authorize(opts);
    } catch (e) {
        next(new MissingPermissionError('putobject'));
        return;
    }

    opts.action = 'putdirectory';
    try {
        req.mahi.authorize(opts);
    } catch (e) {
        next(new MissingPermissionError('putdirectory'));
        return;
    }

    log.debug('ensurePutAccess: done');
    next();
}


function mkdirForJob(req, res, next) {
    function createMd(cb) {
        common.createMetadata(req, 'directory', function (err, md) {
            cb(err, md);
        });
    }

    vasync.parallel({
        funcs: [createMd, createMd, createMd]
    }, function (err, results) {
        if (err) {
            next(err);
            return;
        }

        // req.key here is /:customer_uuid/jobs
        results.successes[0].key = req.key + '/' + req.job.jobId;
        results.successes[1].key = req.key + '/' + req.job.jobId + '/live';
        results.successes[2].key = req.key + '/' + req.job.jobId + '/stor';

        req.log.debug({
            job: req.job
        }, 'mkdirForJob: creating directory stubs');

        vasync.forEachParallel({
            func: req.moray.putMetadata.bind(req.moray),
            inputs: results.successes
        }, function (err2) {
            next(err2);
        });
    });
}


function submitJob(req, res, next) {
    var log = req.log;

    log.debug({job: req.job}, 'submitJob: entered');
    req.marlin.jobCreate(req.job, {log: log}, function (err) {
        if (err) {
            log.debug(err, 'submitJob: failed');
            next(err);
        } else {
            var l = sprintf('/%s/jobs/%s',
                            req.owner.account.login, req.job.jobId);

            log.debug('submitJob: done');
            res.setHeader('Content-Length', '0');
            res.setHeader('Location', l);
            res.send(201);
            next();
        }
    });
}



///--- Exports

module.exports = {
    createHandler: function createHandler() {
        var chain = [
            jobsCommon.jobContextRoot,
            auth.authorizationHandler(),
            ensurePutAccess,
            resumeStream,
            restify.jsonBodyParser({
                mapParams: false,
                maxBodySize: 100000
            }),
            setupJob,
            mkdirForJob,
            submitJob
        ];

        return (chain);
    }
};
