/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

//
// PUT object is probably the most complicated code that happens
// synchronously in muskie, and can get a bit unwieldy to trace through, so
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

var assert = require('assert-plus');
var libmanta = require('libmanta');
var once = require('once');
var restify = require('restify');
var libuuid = require('libuuid');
var vasync = require('vasync');
var VError = require('verror');

var common = require('./common');
var sharkClient = require('./shark_client');
var utils = require('./utils');
require('./errors');

///--- Globals

var clone = utils.shallowCopy;
var httpDate = restify.httpDate;

// Upper bound of 1 million entries in a directory.
var MAX_DIRENTS = 1000000;

/*
 * Default minimum and maximum number of copies of an object we will store,
 * as specified in the {x-}durability-level header.
 *
 * The max number of copies is configurable in the config file; the minimum
 * is not.
 */
var DEF_MIN_COPIES = 1;
var DEF_MAX_COPIES = 9;

// Default number of object copies to store.
var DEF_NUM_COPIES = 2;

// The MD5 sum string for a zero-byte object.
var ZERO_BYTE_MD5 = '1B2M2Y8AsgTpgAmY7PhCfg==';

///--- Helpers

///-- Routes

//--- PUT Handlers ---//

// For `chattr()` support, this function is called by directories as well, so we
// have to special case that and not do all the object stuff.
function parseArguments(req, res, next) {
    if (req.metadata && req.metadata.type === 'directory') {

        /*
         * Clients often inadvertently set the content-type header on PUT.  If
         * it's a request for a directory we assume the client to be
         * well-intentioned but confused, so we silently ignore that they
         * changed the content-type header. We do not do this for objects.
         */
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
        var maxObjectCopies = req.config.maxObjectCopies || DEF_MAX_COPIES;

        // First determine object size
        if (req.isChunked()) {
            var maxSize = req.msk_defaults.maxStreamingSize;
            assert.number(maxSize, 'maxSize');
            len = parseInt(req.header('max-content-length', maxSize), 10);
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
            req._contentMD5 = ZERO_BYTE_MD5;
            req.sharks = [];
            req._zero = true;
            len = 0;
        }

        // Next determine the number of copies
        copies = parseInt((req.header('durability-level') ||
                           req.header('x-durability-level') ||
                           DEF_NUM_COPIES), 10);
        if (copies < DEF_MIN_COPIES || copies > maxObjectCopies) {
            next(new InvalidDurabilityLevelError(DEF_MIN_COPIES,
                maxObjectCopies));
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


// Ensures that directories do not exceed a set number of entries.
function enforceDirectoryCount(req, res, next) {
    if (req.query.metadata) {
        next();
        return;
    }

    assert.ok(req.parentKey);
    var opts = {
        directory: req.parentKey,
        requestId: req.id()
    };

    req.moray.getDirectoryCount(opts, function (err, count, obj) {
        if (err &&
            VError.findCauseByName(err, 'ObjectNotFoundError') === null) {
            next(translateError(err, req));
        } else {
            count = count || 0;

            if (count > MAX_DIRENTS) {
                next(new DirectoryLimitError(req.parentKey));
            } else {
                next();
            }
        }
    });
}


/*
 * Here we add a new object record to Moray.
 */
function addMetadata(req, res, type, next) {
    var log = req.log;
    common.createMetadata(req, type, function (err, opts) {
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
        req.moray.putMetadata(opts, function (err2, obj, data) {
            req.sharks = null;

            if (data !== undefined && data._node) {
                // Record the name of the shard contacted.
                req.entryShard = data._node.pnode;
            }

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


/*
 * Here we save a new object record to Moray.
 */
function saveMetadata(req, res, next) {
    var type = 'object';
    addMetadata(req, res, type, next);
}


/*
 * Here we save a new bucket object record to Moray.
 */
function saveBucketObjectMetadata(req, res, next) {
    var type = 'bucketobject';
    addMetadata(req, res, type, next);
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

    /*
     * Let the delete mechanism know that snaplinks are disabled for this
     * account, so it can treat the object differently
     */
    var uuid = req.owner.account.uuid;
    common.checkAccountSnaplinksEnabled(req, uuid, function (enabled) {
        // Pass in a special header if they are not enabled
        if (!enabled) {
            log.debug({
                link: req.link,
                owner: req.owner.account
            }, 'deletePointer: owner of object has snaplinks disabled');
            opts.snapLinksDisabled = true;
        }
    });
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
    DEF_MIN_COPIES: DEF_MIN_COPIES,
    DEF_MAX_COPIES: DEF_MAX_COPIES,
    DEF_NUM_COPIES: DEF_NUM_COPIES,
    ZERO_BYTE_MD5: ZERO_BYTE_MD5,

    /*
     * This handler is called by the bucket object creation handler. This file
     * provides access to unexposed functions not available to the buckets
     * handler directly which are necessary for writing object data to shards.
     */
    putBucketObjectHandler: function _putBucketObject() {
        var chain = [
            parseArguments,
            common.findSharks,
            common.startSharkStreams,
            common.sharkStreams,
            saveBucketObjectMetadata
        ];
        return (chain);
    },

    putObjectHandler: function _putObject() {
        var chain = [
            restify.conditionalRequest(),
            common.ensureNotRootHandler(),  // not blocking
            parseArguments,  // not blocking
            common.ensureNotDirectoryHandler(), // not blocking
            common.ensureParentHandler(), // not blocking
            enforceDirectoryCount,
            common.findSharks, // blocking
            common.startSharkStreams,
            common.sharkStreams, // blocking
            saveMetadata // blocking
        ];
        return (chain);
    },

    getObjectHandler: function _getObject() {
        var chain = [
            negotiateContent, // not blocking
            restify.conditionalRequest(),
            verifyRange,
            common.streamFromSharks // blocking
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
    },

     // Handlers used for uploading parts to multipart uploads are exposed here.
    putPartHandler: function _putPart() {
        var chain = [
            parseArguments,
            enforceDirectoryCount,
            common.startSharkStreams,
            common.sharkStreams,
            saveMetadata
        ];
        return (chain);
    },

    enforceDirectoryCountHandler: function _enforceDirectoryCount() {
        var chain = [
            enforceDirectoryCount
        ];
        return (chain);
    }
};
