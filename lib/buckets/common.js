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
var sprintf = require('util').format;

require('../errors');

/*
 * A valid bucket name is composed of one or more "labels," separated by
 * periods.
 *
 * A label is defined as a string that meets the following criteria:
 * - Contains only lowercase letters, numbers, and hyphens
 * - Does not start or end with a hyphen.
 *
 * Bucket names must also be between 3 and 63 characters long, and must not
 * "resemble an IP address," as defined immediately below.
 */
var bucketLabelRegexStr = '([a-z0-9]([a-z0-9-]*[a-z0-9])?)';
var bucketRegexStr =
    sprintf('^(%s\\.)*%s$', bucketLabelRegexStr, bucketLabelRegexStr);
var bucketRegex = new RegExp(bucketRegexStr);

/*
 * S3 considers "resembling an IP address" to mean four groups of between one
 * and three digits each, separated by periods. This includes strings that are
 * not actually valid IP addresses. For example:
 *
 * - 1.1.1.1 resembles an IP address
 * - 999.999.999.999 also resembles an IP address
 * - 172.25.1234.1 does not, because there is a section with more than three
 *   digits. This is thus a valid bucket name.
 */
var threeDigitRegexStr = '[0-9]{1,3}';
var resemblesIpRegexStr = sprintf('^%s\.%s\.%s\.%s$', threeDigitRegexStr,
    threeDigitRegexStr, threeDigitRegexStr, threeDigitRegexStr);
var resemblesIpRegex = new RegExp(resemblesIpRegexStr);

function isValidBucketName(name) {
    return bucketRegex.test(name) && !resemblesIpRegex.test(name) &&
        name.length >= 3 && name.length <= 63;
}

function listBuckets(req) {
    return (_list('buckets', req));
}

function listObjects(req, bucket) {
    return (_list('objects', req, bucket));
}

function _list(type, req, bucket) {
    assert.string(type, 'type');
    assert.object(req, 'req');
    assert.object(req.log, 'req.log');
    assert.object(req.boray, 'req.boray');
    assert.object(req.params, 'req.params');
    assert.optionalString(bucket, 'bucket');

    var client = req.boray.client;
    var ee = new EventEmitter();
    var log = req.log;
    var requestId = req.getId();

    var owner = req.owner.account.uuid;
    var sorted = req.params.sorted === 'true';
    var prefix = req.params.prefix || '';
    var order_by = req.params.sort === 'mtime' ? 'created' : 'name';

    var limit;
    if (req.params.limit) {
        limit = parseInt(req.params.limit, 10);
        if (isNaN(limit) || limit <= 0) {
            process.nextTick(function () {
                ee.emit('error', new InvalidLimitError(req.params.limit));
            });
            return (ee);
        }
    } else {
        limit = 0;
    }
    assert.number(limit, 'limit');
    assert.ok(limit >= 0, 'limit >= 0');

    log.debug('buckets common _list (%s): entered', type);

    var mreq;
    switch (type) {
    case 'buckets':
        mreq = client.listBucketsNoVnode(owner, sorted, order_by, prefix,
            limit, requestId);
        break;
    case 'objects':
        mreq = client.listObjectsNoVnode(owner, bucket, sorted, order_by,
            prefix, limit, requestId);
        break;
    default:
        assert.fail('unknown type: ' + type);
        break;
    }

    mreq.on('record', function (r) {
        var entry = {
            name: r.key,
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
    isValidBucketName: isValidBucketName,
    listBuckets: listBuckets,
    listObjects: listObjects
};
