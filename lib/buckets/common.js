/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var EventEmitter = require('events').EventEmitter;

var assert = require('assert-plus');
var jsprim = require('jsprim');

require('../errors');

function listBuckets(req) {
    return _list('buckets', req);
}

function listObjects(req, bucket) {
    return _list('objects', req, bucket);
}

function _list(type, req, bucket) {
    assert.string(type, 'type');
    assert.object(req, 'req');
    assert.object(req.log, 'req.log');
    assert.object(req.moray, 'req.moray');
    assert.object(req.params, 'req.params');
    assert.optionalString(bucket, 'bucket');

    var client = req.moray;
    var ee = new EventEmitter();
    var log = req.log;

    var opts = {
        requestId: req.getId(),
        owner: req.owner.account.uuid,
        sorted: req.params.sorted === 'true',
        prefix: req.params.prefix || '',
        order_by: req.params.sort === 'mtime' ? 'created' : 'name'
    };

    if (req.params.limit) {
        opts.limit = parseInt(req.params.limit, 10);
        if (isNaN(opts.limit) || opts.limit <= 0) {
            process.nextTick(function () {
                ee.emit('error', new InvalidLimitError(req.params.limit));
            });
            return (ee);
        }
    } else {
        opts.limit = 0;
    }
    assert.number(opts.limit, 'opts.limit');
    assert.ok(opts.limit >= 0, 'opts.limit >= 0');

    log.debug('buckets common _list (%s): entered', type);

    var mreq;
    switch (type) {
    case 'buckets':
        mreq = client.listBuckets(opts);
        break;
    case 'objects':
        mreq = client.listObjects(bucket, opts);
        break;
    default:
        assert.fail('unknown type: ' + type);
        break;
    }

    mreq.on('record', function (r) {
        var entry = {
            name: r.key.split('/').pop(),
            etag: r.value.etag,
            size: r.value.contentLength,
            type: r.value.type,
            contentType: r.value.contentType,
            contentMD5: r.value.contentMD5,
            mtime: new Date(r.value.mtime).toISOString()
        };

        if (entry.type === 'object')
            entry.durability = (r.value.sharks || []).length || 0;

        mreq.emit('entry', entry, r);
    });

    return (mreq);
}

///--- Exports

module.exports = {
    listBuckets: listBuckets,
    listObjects: listObjects
};
