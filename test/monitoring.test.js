/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');

var restifyClients = require('restify-clients');
var url = require('url');
var jsprim = require('jsprim');

var before = helper.before;
var test = helper.test;

///--- Tests

before(function (cb) {
    this.kangPath = '/kang/snapshot';
    this.metricsPath = '/metrics';

    /*
     * Since the monitoring server runs on a separate port (http_port + 800),
     * we need to do some slicing and dicing of the user-provided MANTA_URL.
     *
     * Note: This requires that the user follows the README and set MANTA_URL
     * to point to a running Muskie instance. This will fail if MANTA_URL
     * points to a loadbalancer, or doesn't include a port number.
     */

    var parsed_url = url.parse(process.env.MANTA_URL);
    if (typeof (parsed_url.port) !== 'string') {
        cb(new Error('MANTA_URL must include a valid port number'));
        return;
    }

    // Take the :port section of the URL and add 800.
    var port = jsprim.parseInteger(parsed_url.port);
    if (typeof (port) !== 'number') {
        // parseInteger() returned an error, not a number.
        cb(new Error('error parsing MANTA_URL port: ' +  port.message));
        return;
    }
    port += 800;

    parsed_url.port = port;
    parsed_url.host = null;

    // Construct a new url string.
    this.monitor_url = url.format(parsed_url);

    cb();
});

test('kang handler running', function (t) {
    var client = restifyClients.createJsonClient({
        connectTimeout: 250,
        rejectUnauthorized: false,
        retry: false,
        url: this.monitor_url
    });
    client.get(this.kangPath, function (err, req, res, obj) {
        t.ifError(err);
        t.ok(res);
        t.equal(res.statusCode, 200);
        t.ok(obj);
        t.end();
    });
});

test('metric handler running', function (t) {
    var client = restifyClients.createStringClient({
        connectTimeout: 250,
        rejectUnauthorized: false,
        retry: false,
        url: this.monitor_url
    });
    client.get(this.metricsPath, function (err, req, res, data) {
        t.ifError(err);
        t.ok(res);
        t.equal(res.statusCode, 200);
        t.ok(data);
        t.end();
    });
});
