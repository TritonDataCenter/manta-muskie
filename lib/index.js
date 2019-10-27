/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

const apm = require('elastic-apm-node').start({
    serviceName: 'webapi',
    serverUrl: 'http://192.168.7.32:9200'
});

var server = require('./server');
var configure = require('./configure');



///--- Exports

module.exports = {
    configure: configure.configure
};

Object.keys(server).forEach(function (k) {
    module.exports[k] = server[k];
});
