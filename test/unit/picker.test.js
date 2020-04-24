/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var path = require('path');
var test = require('@smaller/tap').test;

var mod_picker = require('../../lib/picker.js');


///--- Constants

const THREE_SIGMA = 0.997;
var DEF_MAX_STREAMING_SIZE_MB = 5120;
var DEF_MAX_PERCENT_UTIL = 90;
var DEF_MAX_OPERATOR_PERCENT_UTIL = 92;


///--- Helpers

/**
 * Sum values in array
 *
 * @param {!number[]} values
 */
function sum(values) {
    return values.reduce(function (acc, v) {
        return (acc += v);
    }, 0);
}

/**
 * Calculate the arithmetic mean of an array of values
 *
 * @param {!number[]} values
 * @returns {number} the sum of values
 */
function mean(values) {
    return (sum(values) / values.length);
}

/**
 * Calculate the standard deviation of an array of values
 *
 * @param {!number[]} values
 * @param {number} [av] - mean, if already calculated
 * @returns {number} the standard deviation of values
 */
function sd(values, av) {
    if (av === undefined) {
        av = mean(values);
    }

    var sqd = values.map(function (v) {
        var res = v - av;
        return (res * res);
    });

    var meansqd = mean(sqd);
    return (Math.sqrt(meansqd));
}

/**
 * Calculate zscore of each element in array of values
 *
 * @param {!number[]} values
 * @param {number} [av] - mean, if already calculated
 * @param {number} [stdd] - standard deviation, if already calculated
 * @returns {number[]} an array of zscores for values
 */
function zscores(values, av, stdd) {
    if (av === undefined) {
        av = mean(values);
    }
    if (stdd === undefined) {
        stdd = sd(values, av);
    }
    return (values.map(function (v) {
        return ((v - av) / stdd);
    }));
}

/**
 * Rough test to see if values follow a normal distribution
 *
 * NOTE: this is non-deterministic, we could do with some Erlang quickcheck
 * ?SOMETIMES magic here (see http://quviq.com/documentation/eqc/ macro
 * ?SOMETIMES for details.)
 *
 * @param {object} kvs - an object of {string} -> {number} mappings
 * @returns {boolean} true if the numbers are most likely normally
 * distributed
 */
function probablyNormal(kvs) {
    var values = Object.keys(kvs).map(function (k) {
        return (kvs[k]);
    });
    var zs = zscores(values);

    // outliers are greater than 3sd from the mean
    var outliers = zs.filter(function (z) {
        return (Math.abs(z) > 3);
    });

    //NOTE: in this case, probably outliers.length === 0 would be
    //enough
    return ((outliers.length / values.length) < (1 - THREE_SIGMA));
}


///--- Tests

test('picker', function (t) {
    var min = Infinity;
    var max = 0;
    var total = 0;
    var runs = 0;
    var dcs = {};
    var hosts = {};

    // Test iterations. Pulled from old lib/picker.js test, see
    // blame/history on that file.
    var N = 10000;

    // Load test data.
    var morayValues = JSON.parse(
        fs.readFileSync(path.resolve(__dirname, 'picker.records.json')));

    // Make a picker.
    var picker = mod_picker.createClient({
        log: require('bunyan').createLogger({
            level: process.env.LOG_LEVEL || 'info',
            name: 'picker_test',
            stream: process.stdout
        }),
        defaultMaxStreamingSizeMB: DEF_MAX_OPERATOR_PERCENT_UTIL,
        maxUtilizationPct: DEF_MAX_PERCENT_UTIL,
        maxOperatorUtilizationPct: DEF_MAX_OPERATOR_PERCENT_UTIL,
        multiDC: true,
        // I.e. don't connect to moray.
        standalone: true
    });

    // Set up the picker with the fake moray data.
    mod_picker.sortAndStoreDcs.call(picker, morayValues, morayValues);

    // do it
    select(t);

    // NOTE: only reports on failure, to aid debugging
    function report() {
        var hostsNormal = probablyNormal(hosts);
        var dcsNormal = probablyNormal(dcs);

        if (!(hostsNormal && dcsNormal)) {
            var keys = Object.keys(hosts);
            keys.sort();
            console.log('**** Host Selection ****');
            keys.forEach(function (k) {
                console.log(k + ' ' + hosts[k]);
            });
            console.log('\n**** Datacenter Selection ****');
            keys = Object.keys(dcs);
            keys.sort();
            keys.forEach(function (k) {
                console.log(k + ' ' + dcs[k]);
            });
            console.log('\n**** Timing ****');
            console.log('avg: ' + total / N  + 'ms');
            console.log('max: ' + max + 'ms');
            console.log('min: ' + min + 'ms');
        }

        picker.close();
        t.ok(hostsNormal, 'Hosts selection appears normally distributed');
        t.ok(dcsNormal, 'DC selection appears normally distributed');
        t.end();
    }

    // the actual test code, yes, it runs iterations
    function select(_t) {
        var start = new Date().getTime();
        picker.choose({}, function onChosen(err, sharks) {
            assert.ifError(err);
            var delta = new Date().getTime() - start;
            if (delta > max)
                max = delta;
            if (delta < min)
                min = delta;
            total += delta;

            Object.keys(sharks).forEach(function track(k) {
                var dc_names = [];
                sharks[k].forEach(function (s) {
                    var id = s.manta_storage_id;
                    if (!hosts[id])
                        hosts[id] = 0;

                    hosts[id]++;

                    dc_names.push(s.datacenter);
                });

                var dc_key = dc_names.join(' ');
                if (!dcs[dc_key])
                    dcs[dc_key] = 0;
                dcs[dc_key]++;
            });

            setImmediate((++runs < N ? select : report), t);
        });
    }
});
