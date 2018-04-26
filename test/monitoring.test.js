/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');

var restify = require('restify');
var url = require('url');
var net = require('net');
var jsprim = require('jsprim');

var before = helper.before;
var test = helper.test;

///--- Tests


before(function (cb) {
    this.kangPath = '/kang/snapshot';
    this.metricsPath = '/metrics';

    var port;

    /*
     * Since the monitoring server runs on a separate port (http_port + 800),
     * we need to do some slicing and dicing of the user-provided MANTA_URL.
     *
     * Note: To run the monitoring tests, the user must set MANTA_URL
     * to point to a running Muskie instance. The tests will fail if MANTA_URL
     * points to a loadbalancer, or doesn't include a port number.
     *
     * The MANTA_URL needs to have an IP address and port number. It could still
     * be the case that the MANTA_URL points to a loadbalancer's IP and port,
     * which will cause the monitoring tests to fail.
     */

    var parsed_url = url.parse(process.env.MANTA_URL);
    if (typeof (parsed_url.port) !== 'string') {
        this.skip = true;
        console.log('skipping monitoring tests: port number not found on' +
            ' MANTA_URL');
        cb();
        return;
    }

    /*
     * If the hostname isn't an IP then we're _probably_ not pointing at a
     * muskie process.
     */
    if (! net.isIP(parsed_url.hostname) &&
            parsed_url.hostname !== 'localhost') {

        this.skip = true;
        console.log('skipping monitoring tests: MANTA_URL is not an IP' +
                ' address and port');
        cb();
        return;
    }

    // Take the :port section of the URL and add 800.
    port = jsprim.parseInteger(parsed_url.port);
    if (typeof (port) !== 'number') {
        // parseInteger() returned an error, not a number.
        this.skip = true;
        console.log('skipping monitoring test: error parsing port number on' +
                ' MANTA_URL');
        cb();
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
    if (this.skip) {
        t.end();
        return;
    }
    var client = restify.createJsonClient({
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
    if (this.skip) {
        t.end();
        return;
    }
    var client = restify.createStringClient({
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
