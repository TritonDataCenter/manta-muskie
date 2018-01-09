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
var jsprim = require('jsprim');

var before = helper.before;
var test = helper.test;

///--- Tests


before(function (cb) {
    this.kangPath = '/kang/snapshot';
    this.metricsPath = '/metrics';

    var numcount = 0;
    var segments;
    var port;
    var err;

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
     * Here we'll do our best to determine if the MANTA_URL is a hostname or
     * an IP address. These tests will have to be skipped if MANTA_URL is a
     * hostname.
     *
     * The method here is pretty simple. The tests are executed if the number of
     * numeric hostname segments equals the total number of hostname segments.
     *
     * Some examples:
     *   172.29.1.100 has four of four number segments, so tests will execute.
     *   1.org has one of two number segments, so tests will be skipped .
     *   mymanta.joyent.com has zero of three number segments, so tests will be
     *     skipped.
     *
     * The only special case is 'localhost'. 'localhost'  is probably what
     * people use when they're testing against a local Muskie repository.
     */
    segments = parsed_url.hostname.split('.');
    segments.forEach(function (part) {
        err = jsprim.parseInteger(part);
        if (err instanceof Error) {
            numcount++;
        }
    });
    if (segments.length === numcount && parsed_url.hostname !== 'localhost') {
        this.skip = true;
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
