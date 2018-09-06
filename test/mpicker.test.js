/*
 * Copyright 2018 Joyent, Inc.
 */

/*
 * Test the "mpicker" command.
 */

var forkExecWait = require('forkexec').forkExecWait;
var path = require('path');

var BINDIR = path.resolve(__dirname, '../bin');
var MPICKER = path.resolve(BINDIR, 'mpicker');


// ---- helper functions

function test(name, testfunc) {
    module.exports[name] = testfunc;
}

// ---- tests

/*
 * Verify command can be invoked without error
 */
test('mpicker -h', function (t) {
    var argv = [
        MPICKER,
        '-h'
    ];

    var usagePhrase = 'Models the behavior of the Muskie "picker" component.';

    forkExecWait({
        argv: argv
    }, function (err, info) {
        t.ifError(err, err);

        t.equal(info.stderr, '', 'no stderr');
        t.equal(info.stdout.lastIndexOf(usagePhrase, 0), 0,
            'stdout from mpicker');

        t.done();
    });
});
