#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var fs = require('fs');
var path = require('path');

var dashdash = require('dashdash');

var auth = require('../lib/auth');



///--- Globals

var CLI_OPTS = [
    {
        names: ['file', 'f'],
        type: 'string',
        help: 'Configuration file to use.',
        helpArg: 'FILE',
        'default': path.resolve(__dirname, '../etc/config.json')
    },
    {
        names: ['help', 'h'],
        type: 'bool',
        help: 'Print this help and exit.'
    }
];
var PROG = path.basename(__filename);



///--- Mainline

(function main() {
    var parser = dashdash.createParser({options: CLI_OPTS});
    try {
        var opts = parser.parse(process.argv);
    } catch (e) {
        console.error('%s: error: %s', PROG, e.toString());
        process.exit(1);
    }

    if (opts.help) {
        var help = parser.help({includeEnv: true}).trimRight();
        console.log('usage: %s [OPTIONS] TOKEN\noptions:\n%s', PROG, help);
        process.exit(0);
    }

    try {
        var data = fs.readFileSync(opts.file, 'utf8');
        var cfg = JSON.parse(data);
    } catch (e) {
        console.error('%s: error parsing %s: %s', PROG,
                      opts.file, e.toString());
        process.exit(1);
    }

    if (!opts._args.length) {
        console.error('%s: token required', PROG);
        process.exit(1);
    }

    (function parse(ndx) {
        var t = opts._args[ndx];
        auth.parseAuthToken(t, cfg.authToken, function (err, parsed) {
            if (err) {
                console.error('%s: failed to parse token: %s', PROG,
                              err.toString());
                process.exit(1);
            }

            console.log(JSON.stringify(parsed, null, 4));

            if (opts._args.length > ++ndx)
                parse(ndx);
        });
    })(0);
})();
