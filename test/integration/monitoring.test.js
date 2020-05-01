/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var jsprim = require('jsprim');
var restifyClients = require('restify-clients');
var test = require('tap').test;
var url = require('url');
var VError = require('verror');

// There is a monitoring server at port `http_port + 800` for each muskie
// process. We can't use $MANTA_URL in general, because that may be the
// *loadbalancer* service host or IP. There is always at least a webapi server
// at port 8081, hence always a monitoring server at port 8881.
var monitorUrl = 'http://localhost:8881';


///--- Tests

test('monitoring', function (suite) {
    test('kang handler running', function (t) {
        var client = restifyClients.createJsonClient({
            connectTimeout: 2000,
            rejectUnauthorized: false,
            retry: false,
            url: monitorUrl
        });
        client.get('/kang/snapshot', function (err, req, res, obj) {
            t.ifError(err, 'no error from kang endpoint');
            t.equal(res.statusCode, 200, '200 HTTP response code from kang');
            t.ok(obj, 'got a kang object');
            t.end();
        });
    });

    test('metric handler running', function (t) {
        var client = restifyClients.createStringClient({
            connectTimeout: 2000,
            rejectUnauthorized: false,
            retry: false,
            url: monitorUrl
        });
        client.get('/metrics', function (err, req, res, data) {
            t.ifError(err, 'no error from /metrics');
            t.equal(res.statusCode, 200,
                '200 HTTP response code from /metrics');
            t.ok(data, 'got data from /metrics');
            t.end();
        });
    });

    suite.end();
});
