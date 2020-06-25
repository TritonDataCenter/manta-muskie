/*
 * Copyright 2020 Joyent, Inc.
 */

/*
 * Test the "mpicker" command.
 */

var forkExecWait = require('forkexec').forkExecWait;
var path = require('path');
var test = require('tap').test;

var MPICKER = path.resolve(__dirname, '../../bin/mpicker');


// ---- tests

// Verify command can be invoked without error.
test('mpicker -h', function (t) {
    var argv = [
        MPICKER,
        '-h'
    ];

    var usagePhrase = 'Models the behavior of the Muskie "picker" component.';

    forkExecWait({
        argv: argv
    }, function (err, info) {
        t.ifError(err, 'invoked "mpicker -h" without error');

        t.equal(info.stderr, '', 'no stderr');
        t.equal(info.stdout.lastIndexOf(usagePhrase, 0), 0,
            'stdout from mpicker includes expected usage phrase');

        t.end();
    });
});
