/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var assert = require('assert-plus');
var common = require('../common');
var errors = require('../errors');

function loadRequest(req, res, next) {

    var resource = {};
    var requestType;
    req.metadata = {};

    if (req.params.bucket_name) {
        req.bucket = new Bucket(req);
        requestType = 'bucket';
        resource.key = req.bucket.name;
    }

    if (req.params.object_name) {
        req.bucketObject = new BucketObject(req);
        requestType = 'object';
        resource.key = req.bucketObject.name;
    }

    resource.owner = req.owner;

    switch (req.method) {
    case 'GET':
        /* falls through */
    case 'HEAD':
        req.authContext.action = 'get' + requestType;
        break;
    case 'DELETE':
        req.authContext.action = 'delete' + requestType;
        break;
    default:
        req.authContext.action = 'put' + requestType;
        break;
    }

    // TODO: Populate roles from headers
    resource.roles = [];
    req.authContext.resource = resource;

    next();

}

/* This is a function used before bucket object operations */
function getBucketIfExists(req, res, next) {
    var owner = req.owner.account.uuid;
    var bucket = req.bucket;
    var log = req.log;

    log.debug({
        owner: owner,
        bucket: bucket.name
    }, 'getBucketIfExists: requested');

    var onGetBucket = function onGet(err, bucket_data) {
        if (err) {
            log.debug({
                err: err,
                owner: owner,
                bucket: bucket.name
            }, 'getBucketIfExists: failed');
            next(err);
            return;
        } else {
            if (bucket_data.error && bucket_data.error.name ===
                'BucketNotFound') {
                req.resource_exists = false;
                var not_found_error =
                    new errors.BucketNotFoundError(bucket.name);
                next(not_found_error);
            } else {
                log.debug({
                    owner: owner,
                    bucket: bucket.name
                }, 'getBucketIfExists: done');
                req.bucket.id = bucket_data.id;

                next(null, bucket_data);
            }
        }
    };

    req.boray.client.getBucketNoVnode(owner, bucket.name, onGetBucket);
}

function Bucket(req) {

    var self = this;

    assert.object(req, 'req');
    if (req.params.bucket_name) {
        self.name = req.params.bucket_name;
    }
    self.type = 'bucket';

    return (self);

}

function BucketObject(req) {

    var self = this;

    assert.object(req, 'req');
    assert.string(req.params.bucket_name, 'req.params.bucket_name');
    self.bucket_name = req.params.bucket_name;
    if (req.params.object_name) {
        self.name = req.params.object_name;
    }
    self.type = 'bucketobject';

    return (self);

}


// TODO: Break this up into smaller pieces
function createObjectMetadata(req, type, cb) {
    var names;
    var md = {
        headers: {},
        roles: [],
        type: 'bucketobject'
    };

    common.CORS_RES_HDRS.forEach(function (k) {
        var h = req.header(k);
        if (h) {
            md.headers[k] = h;
        }
    });

    if (req.headers['cache-control'])
        md.headers['Cache-Control'] = req.headers['cache-control'];

    if (req.headers['surrogate-key'])
        md.headers['Surrogate-Key'] = req.headers['surrogate-key'];

    var hdrSize = 0;
    Object.keys(req.headers).forEach(function (k) {
        if (/^m-\w+/.test(k)) {
            hdrSize += Buffer.byteLength(req.headers[k]);
            if (hdrSize < common.MAX_HDRSIZE)
                md.headers[k] = req.headers[k];
        }
    });

    md.contentLength = req._size;
    md.contentMD5 = req._contentMD5;
    md.contentType = req.header('content-type') ||
        'application/octet-stream';
    md.objectId = req.objectId;

    if (md.contentLength === 0) { // Chunked requests
        md.sharks = [];
    } else if (req.sharks && req.sharks.length) { // Normal requests
        md.sharks = req.sharks.map(function (s) {
            return ({
                datacenter: s._shark.datacenter,
                manta_storage_id: s._shark.manta_storage_id
            });
        });
    } else { // Take from the prev is for things like mchattr
        md.sharks = [];
    }

    // mchattr
    var requestedRoleTags;
    if (req.auth && typeof (req.auth['role-tag']) === 'string') { // from URL
        requestedRoleTags = req.auth['role-tag'];
    } else {
        requestedRoleTags = req.headers['role-tag'];
    }

    if (requestedRoleTags) {
        /* JSSTYLED */
        names = requestedRoleTags.split(/\s*,\s*/);
        req.mahi.getUuid({
            account: req.owner.account.login,
            type: 'role',
            names: names
        }, function (err, lookup) {
            if (err) {
                cb(err);
                return;
            }
            var i;
            for (i = 0; i < names.length; i++) {
                if (!lookup.uuids[names[i]]) {
                    cb(new InvalidRoleTagError(names[i]));
                    return;
                }
                md.roles.push(lookup.uuids[names[i]]);
            }
            cb(null, md);
        });
    // apply all active roles if no other roles are specified
    } else if (req.caller.user) {
        md.roles = req.activeRoles;
        setImmediate(function () {
            cb(null, md);
        });
    } else {
        setImmediate(function () {
            cb(null, md);
        });
    }
}

function notFoundHandler(req, res, next) {
    if (req.not_found_error) {
        next(req.not_found_error);
    } else {
        next();
    }
}

function successHandler(req, res, next) {
    var owner = req.owner.account.uuid;
    var log = req.log;

    log.debug({
        owner: owner
    }, 'successHandler: entered');

    if (req.method == 'PUT' || req.method == 'POST' || req.method == 'DELETE') {
        res.send(204);
    } else {
        res.send(200);
    }

    log.debug({
        owner: owner
    }, 'successHandler: done');

    next();
}

function isConditional(req) {
    return (req.headers['if-match'] !== undefined ||
            req.headers['if-none-match'] !== undefined ||
            req.headers['if-modified-since'] !== undefined ||
            req.headers['if-unmodified-since'] !== undefined);
}

module.exports = {
    Bucket: Bucket,
    BucketObject: BucketObject,
    getBucketIfExists: getBucketIfExists,
    createObjectMetadata: createObjectMetadata,
    loadRequest: loadRequest,
    notFoundHandler: notFoundHandler,
    successHandler: successHandler,
    isConditional: isConditional
};
