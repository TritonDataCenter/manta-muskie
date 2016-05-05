/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

//
// PUT object is probably the most complicated code that happens
// synchronously in muskie, and can get a bit uniwiedly to trace through, so
// below is some context on what we need to do.
//
// Recall that the contract of PUT object is that by default muskie will stream
// data to 2 backend ZFS hosts in two discreet datacenters (assuming there
// exists > 1 datacenter).  In addition, muskie offers test/set semantics on
// etag, so we need to factor that in as part of this sequence as well.  First,
// let's list the set of steps that happen (in english):
//
// 0 pause the incoming data stream
// 1 Authenticate/Authorize the end user (handled beforehand)
// 2 Look for existing metadata of the current key AND the parent dir
// 3 Ensure parent_dir exists
// 4 If the user sent an etag, enforce it lines up, whether that means
//   the previous value (or null for creation)
// 5 Find makos to place the raw bytes on
// 6 Attempt to connect to $num_copies makos spread across DCs
// 7 Stream data to makos from above (and unpause the request)
// 8 Validate the MD5 we got was what the client requested
// 9 Store the new metadata record back into moray
//
// Now the most interesting steps are 5/6 and 9.
//
// Step 5 we use picker.choose (see picker.js) and get three distinct
// sets of sharks to connect to.  In the normal case each set has two sharks
// in two different DCs ("other" cases are when we have > 2 replicas). This is
// some weird derivation of "power of two" load balancing, where we try ALL in
// primary, and if ANY fail, we try ALL in secondary, and then either proceed
// or bail. Oh, and we have "3" sets :).  The number "3" is distinctly chosen
// as we always have manta in at least 3 datacenters in production; since manta
// is guaranteed to operate when any 2 are up, we want 3 choose 2 tuples.
//
// Lastly step 9: step 9 requires that we do a conditional request to moray
// to save back metadata and not retry IFF the user sent an etag on the input;
// If there's a mismatch, we'll return ConcurrentRequestError, otherwise we
// retry once.
//
// In terms of some nitty/gritty details: there's a pile of crap that gets
// tacked onto the `req` object along the way, specifically the current
// metadata record, the set of sharks to connect to, the outstanding shark
// requests, the number of copies, the size, etc.

var crypto = require('crypto');
var util = require('util');

var assert = require('assert-plus');
var libmanta = require('libmanta');
var once = require('once');
var restify = require('restify');
var libuuid = require('libuuid');
var vasync = require('vasync');

var common = require('./common');
var CheckStream = require('./check_stream');
var sharkClient = require('./shark_client');
var utils = require('./utils');
require('./errors');



///--- Globals

var clone = utils.shallowCopy;
var httpDate = restify.httpDate;
var sprintf = util.format;

var DATA_TIMEOUT = parseInt(process.env.MUSKIE_DATA_TIMEOUT || 45000, 10);
var DEF_MAX_LEN = 53687091200;



///--- Helpers

// Simple wrapper around sharkClient.getClient + put
//
// opts ->
//   {
//      contentType: req.getContentType(),   // content-type from the request
//      contentLength: req.isChunked() ? undefined : req._size,
//      log: $bunyan,
//      shark: $shark,  // a specific shark from $picker.choose()
//      objectId: req.objectId,    // proposed objectId
//      owner: req.owner.account.uuid,   // /:login/stor/... (uuid for $login)
//      sharkConfig: {  // from config.json
//        connectTimeout: 4000,
//        retry: {
//          retries: 2
//        }
//      },
//      requestId: req.getId()   // current request_id
//   }
//
function sharkConnect(opts, cb) {
    var client = sharkClient.getClient({
        connectTimeout: opts.sharkConfig.connectTimeout,
        log: opts.log,
        retry: opts.sharkConfig.retry,
        shark: opts.shark
    });
    assert.ok(client, 'sharkClient returned null');

    client.put(opts, function (err, req) {
        if (err) {
            cb(err);
        } else {
            req._shark = opts.shark;
            opts.log.debug({
                client_req: req
            }, 'SharkClient: put started');
            cb(null, req);
        }
    });
}



///-- Routes

//--- PUT Handlers ---//

// For `chattr()` support, this function is called by directories as well, so we
// have to special case that and not do all the object stuff.
function parseArguments(req, res, next) {
    if (req.metadata && req.metadata.type === 'directory') {

        // This is ghetto, but clients often inadvertently set the content-type
        // header on PUT, so if it's a directory we assume them to be
        // well-intentioned, but stupid. So we just silently ignore that they
        // changed it. This is not true of objects, so we don't help them out
        // there.
        var _ct = req.headers['content-type'];
        if (_ct && _ct !== 'application/json; type=directory')
            req.headers['content-type'] = 'application/json; type=directory';

        if (![
            'content-length',
            'content-md5',
            'durability-level'
        ].some(function (k) {
            var bad = req.headers[k];
            if (bad) {
                setImmediate(function killRequest() {
                    next(new InvalidUpdateError(k, ' on a directory'));
                });
            }
            return (bad);
        })) {
            next();
        }
        return;
    } else {
        var copies;
        var len;
        var maxObjectCopies = req.config.maxObjectCopies;

        // First determine object size
        if (req.isChunked()) {
            len = parseInt(req.header('max-content-length', DEF_MAX_LEN),
                           10);
            if (len < 0) {
                next(new MaxContentLengthError(len));
                return;
            }
            req.log.debug('streaming upload: using max_size=%d', len);
        } else if ((len = req.getContentLength()) < 0) {
            // allow zero-byte objects
            next(new ContentLengthError());
            return;
        } else if ((req.getContentLength() || 0) === 0) {
            req._contentMD5 = '1B2M2Y8AsgTpgAmY7PhCfg==';
            req.sharks = [];
            req._zero = true;
            len = 0;
        }

        // Next determine the number of copies
        copies = parseInt((req.header('durability-level') ||
                           req.header('x-durability-level') ||
                           2), 10);
        if (copies < 1 || copies > (maxObjectCopies || 9)) {
            next(new InvalidDurabilityLevelError(1, maxObjectCopies));
            return;
        }

        if (!req.query.metadata) {
            req._copies = copies;
            req._size = len;
            req.objectId = libuuid.create();
            assert.ok(len >= 0);
            assert.ok(copies >= 0);
            assert.ok(req.objectId);
        } else {
            if ([
                'content-length',
                'content-md5',
                'durability-level'
            ].some(function (k) {
                var bad = req.headers[k];
                if (bad) {
                    setImmediate(function killRequest() {
                        next(new InvalidUpdateError(k));
                    });
                }
                return (bad);
            })) {
                return;
            }

            // Ensure the object we're updating actually exists
            if (!req.metadata || !req.metadata.type) {
                next(new ResourceNotFoundError(req.path()));
                return;
            }
        }

        req.log.debug({
            copies: req._copies,
            length: req._size
        }, 'putObject:parseArguments: done');
        next();
    }

}


function findSharks(req, res, next) {
    if (req._zero || req.query.metadata) {
        next();
        return;
    }

    var log = req.log;
    var opts = {
        replicas: req._copies,
        requestId: req.getId(),
        size: req._size
    };

    log.debug(opts, 'findSharks: entered');

    opts.log = req.log;
    req.picker.choose(opts, function (err, sharks) {
        if (err) {
            next(err);
        } else {
            req._sharks = sharks;
            log.debug({
                sharks: req._sharks
            }, 'findSharks: done');
            next();
        }
    });
}


function enforceDirectoryCount(req, res, next) {
    if (req.query.metadata) {
        next();
        return;
    }

    var opts = {
        directory: req.parentKey,
        requestId: req.id()
    };

    req.moray.getDirectoryCount(opts, function (err, count, obj) {
        if (err && err.name !== 'ObjectNotFoundError') {
            next(translateError(err, req));
        } else {
            count = count || 0;

            // Enforce a 1M directory limit
            if (count > 1000000) {
                next(new DirectoryLimitError(req.parentKey));
            } else {
                next();
            }
        }
    });
}


function startSharkStreams(req, res, next) {
    if (req._zero || req.query.metadata) {
        next();
        return;
    }

    var log = req.log;

    log.debug({
        objectId: req.objectId,
        sharks: req._sharks
    }, 'startSharkStreams: entered');

    var ndx = 0;
    var opts = {
        contentType: req.getContentType(),
        contentLength: req.isChunked() ? undefined : req._size,
        contentMd5: req.headers['content-md5'],
        objectId: req.objectId,
        owner: req.owner.account.uuid,
        requestId: req.getId(),
        sharkConfig: req.sharkConfig
    };

    (function attempt(inputs) {
        vasync.forEachParallel({
            func: function shark_connect(shark, cb) {
                var _opts = clone(opts);
                _opts.log = req.log;
                _opts.shark = shark;
                sharkConnect(_opts, cb);
            },
            inputs: inputs
        }, function (err, results) {
            req.sharks = results.successes || [];
            if (err || req.sharks.length < req._copies) {
                log.debug({
                    err: err,
                    sharks: inputs
                }, 'startSharkStreams: failed');

                req.abandonSharks();
                if (ndx < req._sharks.length) {
                    attempt(req._sharks[ndx++]);
                } else {
                    next(new SharksExhaustedError(res));
                }
                return;
            }
            if (log.debug()) {
                req.sharks.forEach(function (s) {
                    s.headers = s._headers;
                    log.debug({
                        client_req: s
                    }, 'mako: stream started');
                });

                log.debug({
                    objectId: req.objectId,
                    sharks: inputs
                }, 'startSharkStreams: done');
            }
            next();
        });
    })(req._sharks[ndx++]);
}


function sharkStreams(req, res, next) {
    if (req._zero || req.query.metadata) {
        next();
        return;
    }

    var next_err = once(function _next_err(err) {
        req.log.debug({
            err: err
        }, 'abandoning request');

        req.removeListener('end', onEnd);
        req.removeListener('error', next_err);

        req.abandonSharks();
        req.unpipe(check);
        check.abandon();

        next(err);
    });

    var barrier = vasync.barrier();
    var check = new CheckStream({
        algorithm: 'md5',
        maxBytes: req._size,
        timeout: DATA_TIMEOUT
    });
    var log = req.log;

    req.domain.add(check);

    barrier.once('drain', function onCompleteStreams() {
        req._timeToLastByte = Date.now();

        req.connection.removeListener('error', abandonUpload);
        req.removeListener('error', next_err);

        if (req.sharks.some(function (s) {
            return (s.md5 !== check.digest('base64'));
        })) {
            var _md5s = req.sharks.map(function (s) {
                return (s.md5);
            });
            log.error({
                clientMd5: req.headers['content-md5'],
                muskieMd5: check.digest('base64'),
                makoMd5: _md5s
            }, 'mako didnt recieve what muskie sent');
            var m = sprintf('muskie md5 %s and mako md5 ' +
                            '%s don\'t match', check.digest('base64'),
                            _md5s.join());
            next_err(new InternalError(m));
        } else {
            log.debug('sharkStreams: done');
            next();
        }
    });

    log.debug('streamToSharks: streaming data');

    function abandonUpload() {
        next_err(new UploadAbandonedError());
    }

    req.connection.once('error', abandonUpload);

    req.once('error', next_err);

    barrier.start('client');
    req.pipe(check);
    req.sharks.forEach(function (s) {
        barrier.start(s._shark.manta_storage_id);
        req.pipe(s);
        s.once('response', function onSharkResult(sres) {
            log.debug({
                mako: s._shark.manta_storage_id,
                client_res: sres
            }, 'mako: response received');

            s.md5 = sres.headers['x-joyent-computed-content-md5'] ||
                req._contentMD5;
            if (sres.statusCode === 469) {
                next_err(new ChecksumError(s.md5, req.headers['content-md5']));
            } else if (sres.statusCode === 400 && req.headers['content-md5']) {
                next_err(new restify.BadRequestError('Content-MD5 invalid'));
            } else if (sres.statusCode > 400) {
                var body = '';
                sres.setEncoding('utf8');
                sres.on('data', function (chunk) {
                    body += chunk;
                });
                sres.once('end', function () {
                    log.debug({
                        mako: s._shark.manta_storage_id,
                        client_res: sres,
                        body: body
                    }, 'mako: response error');
                    next_err(new InternalError());
                });
                sres.once('error', function (err) {
                    next_err(new InternalError(err));
                });
            } else {
                barrier.done(s._shark.manta_storage_id);
            }
        });
    });

    check.once('timeout', function () {
        res.header('connection', 'close');
        next_err(new UploadTimeoutError());
    });

    check.once('length_exceeded', function (sz) {
        next_err(new MaxSizeExceededError(sz));
    });

    check.once('error', next_err);

    function onEnd() {
        // We replace the actual size, in case it was streaming, and
        // the content-md5 we actually calculated on the wire
        req._contentMD5 = check.digest('base64');
        req._size = check.bytes;
        barrier.done('client');
    }

    req.once('end', onEnd);

    barrier.start('check_stream');
    check.once('done', function () {
        barrier.done('check_stream');
    });

    if (req.header('expect') === '100-continue') {
        res.writeContinue();
        log.info({
            remoteAddress: req.connection._xff,
            remotePort: req.connection.remotePort,
            req_id: req.id,
            latency: (Date.now() - req._time),
            'audit_100': true
        }, '100-continue sent');
    }

    req._timeAtFirstByte = Date.now();
}


function saveMetadata(req, res, next) {
    var log = req.log;
    common.createMetadata(req, 'object', function (err, opts) {
        if (err) {
            next(err);
            return;
        }
        opts.etag = opts.objectId;
        opts.previousMetadata = req.metadata;

        if (req.isPublicPut() && !opts.headers['access-control-allow-origin'])
            opts.headers['access-control-allow-origin'] = '*';

        log.debug({
            options: opts
        }, 'saveMetadata: entered');
        req.moray.putMetadata(opts, function (err2) {
            req.sharks = null;
            if (err2) {
                log.debug(err2, 'saveMetadata: failed');
                next(err2);
            } else {
                log.debug('saveMetadata: done');
                if (req.headers['origin']) {
                    res.header('Access-Control-Allow-Origin',
                               req.headers['origin']);
                }
                res.header('Etag', opts.etag);
                res.header('Last-Modified', new Date(opts.mtime));
                res.header('Computed-MD5', req._contentMD5);
                res.send(204);
                next();
            }
        });
    });
}



//-- GET Handlers --//

function negotiateContent(req, res, next) {
    if (req.metadata.type !== 'object')
        return (next());

    var type = req.metadata.contentType;
    if (!req.accepts(type))
        return (next(new NotAcceptableError(req, type)));

    return (next());
}


function verifyRange(req, res, next) {
    if (!req.headers || !req.headers['range'])
        return (next());

    //Specifically disallow multi-range headers.
    var range = req.headers['range'];
    if (range.indexOf(',') !== -1) {
        var message = 'multi-range requests not supported';
        return (next(new NotImplementedError(message)));
    }

    return (next());
}


// Here we pick a shark to talk to, and the first one that responds we
// just stream from. After that point any error is an internal error.
function streamFromSharks(req, res, next) {
    if (req.metadata.type !== 'object') {
        next();
        return;
    }

    var connected = false;
    var log = req.log;
    var md = req.metadata;
    var opts = {
        owner: req.owner.account.uuid,
        creator: md.creator,
        objectId: md.objectId,
        requestId: req.getId()
    };
    var queue;
    var saved_err = false;

    if (req.headers.range)
        opts.range = req.headers.range;

    log.debug('streamFromSharks: entered');

    common.addCustomHeaders(req, res);

    if (md.contentLength === 0 || req.method === 'HEAD') {
        log.debug('streamFromSharks: HEAD || zero-byte object');
        res.header('Durability-Level', req.metadata.sharks.length);
        res.header('Content-Length', md.contentLength);
        res.header('Content-MD5', md.contentMD5);
        res.header('Content-Type', md.contentType);
        res.send(200);
        next();
        return;
    }

    function respond(shark, shark_req) {
        log.debug('streamFromSharks: streaming data');
        // Response headers
        var sh = shark.headers;
        if (req.headers['range'] !== undefined) {
            res.header('Content-Type', sh['content-type']);
            res.header('Content-Range', sh['content-range']);
        } else {
            res.header('Accept-Ranges', 'bytes');
            res.header('Content-Type', md.contentType);
            res.header('Content-MD5', md.contentMD5);
        }

        res.header('Content-Length', sh['content-length']);
        res.header('Durability-Level', req.metadata.sharks.length);

        req._size = sh['content-length'];

        // Response body
        req._totalBytes = 0;
        var check = new CheckStream({
            maxBytes: parseInt(sh['content-length'], 10) + 1024,
            timeout: DATA_TIMEOUT
        });
        check.once('done', function onCheckDone() {
            req.connection.removeListener('error', onConnectionClose);

            if (check.digest('base64') !== md.contentMD5 &&
                !req.headers.range) {
                // We can't set error now as the header has already gone out
                // MANTA-1821, just stop logging this for now XXX
                log.warn({
                    expectedMD5: md.contentMD5,
                    returnedMD5: check.digest('base64'),
                    expectedBytes: parseInt(sh['content-length'], 10),
                    computedBytes: check.bytes,
                    url: req.url
                }, 'GetObject: partial object returned');
                res.statusCode = 597;
            }

            log.debug('streamFromSharks: done');
            req._timeAtFirstByte = check.start;
            req._timeToLastByte = Date.now();
            req._totalBytes = check.bytes;
            next();
        });
        shark.once('error', next);

        function onConnectionClose() {
            // MANTA-1994
            req._probes.client_close.fire(function onFire() {
                var _obj = {
                    id: req._id,
                    method: req.method,
                    headers: req.headers,
                    url: req.url,
                    bytes_sent: check.bytes,
                    bytes_expected: parseInt(sh['content-length'], 10)
                };
                return ([_obj]);
            });

            check.removeAllListeners('done');
            shark.unpipe(check);
            shark.unpipe(res);
            shark_req.abort();
            req._timeAtFirstByte = check.start;
            req._timeToLastByte = Date.now();
            req._totalBytes = check.bytes;
            res.statusCode = 499;
            next(false);
        }

        req.connection.once('error', onConnectionClose);

        res.writeHead(shark.statusCode);
        shark.pipe(check);
        shark.pipe(res);
    }

    queue = libmanta.createQueue({
        limit: 1,
        worker: function start(s, cb) {
            if (connected) {
                cb();
            } else {
                s.get(opts, function (err, c_req, c_res) {
                    if (err) {
                        log.warn({
                            err: err,
                            shark: s.toString()
                        }, 'mako: connection failed');
                        saved_err = err;
                        cb();
                    } else {
                        connected = true;
                        respond(c_res, c_req);
                        cb();
                    }
                });
            }
        }
    });

    queue.once('end', function () {
        if (!connected) {
            // Honor Nginx handling Range GET requests
            if (saved_err && saved_err._result) {
                var rh = saved_err._result.headers;
                if (req.headers['range'] !== undefined && rh['content-range']) {
                    res.setHeader('content-range', rh['content-range']);
                    next(new restify.RequestedRangeNotSatisfiableError());
                    return;
                }
            }
            next(saved_err || new InternalError());
        }
    });

    req.metadata.sharks.forEach(function (s) {
        queue.push(sharkClient.getClient({
            connectTimeout: req.sharkConfig.connectTimeout,
            log: req.log,
            retry: req.sharkConfig.retry,
            shark: s
        }));
    });

    queue.close();
}



//-- DELETE handlers --//

function deletePointer(req, res, next) {
    if (req.metadata.type !== 'object')
        return (next());

    var log = req.log;
    var opts = {
        key: req.key,
        _etag: req.isConditional() ? req.metadata._etag : undefined,
        requestId: req.getId(),
        previousMetadata: req.metadata
    };

    log.debug(opts, 'deletePointer: entered');
    req.moray.delMetadata(opts, function (err) {
        if (err) {
            next(err);
        } else {
            log.debug('deletePointer: done');
            res.send(204);
            next();
        }
    });
    return (undefined);
}



///--- Exports

module.exports = {


    putObjectHandler: function _putObject() {
        var chain = [
            restify.conditionalRequest(),
            common.ensureNotRootHandler(),  // not blocking
            parseArguments,  // not blocking
            common.ensureNotDirectoryHandler(), // not blocking
            common.ensureParentHandler(), // not blocking
            enforceDirectoryCount,
            findSharks, // blocking
            startSharkStreams,
            sharkStreams, // blocking
            saveMetadata // blocking
        ];
        return (chain);
    },

    getObjectHandler: function _getObject() {
        var chain = [
            negotiateContent, // not blocking
            restify.conditionalRequest(),
            verifyRange,
            streamFromSharks // blocking
        ];

        return (chain);
    },

    deleteObjectHandler: function _delObject() {
        var chain = [
            common.ensureNotRootHandler(),
            restify.conditionalRequest(),
            deletePointer
        ];
        return (chain);
    }
};
