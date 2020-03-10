/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var bunyan = require('bunyan');
var configure = require('../lib/configure.js');

exports.configureLogging = function (t) {
    var appName = 'muskie';
    var bunyanCfg1 = {
        level: 'info'
    };
    var bunyanCfg2 = {
        level: 'debug'
    };

    // Record the value of LOG_LEVEL so it can be restored after testing
    var startingLogLevel = process.env.LOG_LEVEL || '';

    // Unset the LOG_LEVEL to set a known starting point for the tests
    process.env.LOG_LEVEL = '';

    var logObj1 = configure.configureLogging(appName, bunyanCfg1, [true]);

    t.equal(logObj1.streams.length, 1);
    t.equal(logObj1.level(), bunyan.DEBUG);
    t.equal(logObj1.src, true);

    var logObj2 = configure.configureLogging(appName, bunyanCfg1, null);

    t.equal(logObj2.streams.length, 2);
    var logObject2Levels = logObj2.levels().sort();
    t.equal(logObject2Levels[0], bunyan.DEBUG);
    t.equal(logObject2Levels[1], bunyan.INFO);
    t.equal(logObj2.src, false);

    var logObj3 = configure.configureLogging(appName, bunyanCfg2, null);

    t.equal(logObj3.streams.length, 1);
    t.equal(logObj3.level(), bunyan.DEBUG);
    t.equal(logObj3.src, true);

    process.env.LOG_LEVEL = 'warn';
    var logObj4 = configure.configureLogging(appName, bunyanCfg2, null);

    t.equal(logObj4.streams.length, 2);
    var logObject4Levels = logObj4.levels().sort();
    t.equal(logObject4Levels[0], bunyan.DEBUG);
    t.equal(logObject4Levels[1], bunyan.WARN);
    t.equal(logObj4.src, false);

    process.env.LOG_LEVEL = 'debug';
    var logObj5 = configure.configureLogging(appName, bunyanCfg1, null);

    t.equal(logObj5.streams.length, 1);
    t.equal(logObj5.level(), bunyan.DEBUG);
    t.equal(logObj5.src, true);

    var logObj6 = configure.configureLogging(appName, bunyanCfg1, [true, true]);

    t.equal(logObj6.streams.length, 1);
    t.equal(logObj6.level(), bunyan.TRACE);
    t.equal(logObj6.src, true);

    process.env.LOG_LEVEL = 'info';
    var logObj7 = configure.configureLogging(appName, bunyanCfg1, [true, true]);

    t.equal(logObj7.streams.length, 1);
    t.equal(logObj7.level(), bunyan.TRACE);
    t.equal(logObj7.src, true);

    process.env.LOG_LEVEL = 'info';
    var logObj12 = configure.configureLogging(appName, null, null);

    t.equal(logObj12.streams.length, 2);
    var logObject12Levels = logObj12.levels().sort();
    t.equal(logObject12Levels[0], bunyan.DEBUG);
    t.equal(logObject12Levels[1], bunyan.INFO);
    t.equal(logObj12.src, false);

    var logObj13 = configure.configureLogging(appName, null, [true]);

    t.equal(logObj13.streams.length, 1);
    t.equal(logObj13.level(), bunyan.DEBUG);
    t.equal(logObj13.src, true);

    process.env.LOG_LEVEL = 'debug';
    var logObj14 = configure.configureLogging(appName, null, null);

    t.equal(logObj14.streams.length, 1);
    t.equal(logObj14.level(), bunyan.DEBUG);
    t.equal(logObj14.src, true);

    var logObj15 = configure.configureLogging(appName, null, [true]);

    t.equal(logObj15.streams.length, 1);
    t.equal(logObj15.level(), bunyan.TRACE);
    t.equal(logObj15.src, true);

    // Restore LOG_LEVEL to the setting prior to the execution of the tests
    process.env.LOG_LEVEL = startingLogLevel;

    t.done();
};
