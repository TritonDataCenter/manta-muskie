/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

// Test operations on directories related to *operator-only* access.

var assert = require('assert-plus');
var test = require('tap').test;
var uuidv4 = require('uuid/v4');
var vasync = require('vasync');

var helper = require('../helper');



///--- Globals

var assertMantaRes = helper.assertMantaRes;


///--- Tests

var client;
var operClient;
var storDir;
var stringClient;
var testAccount;
var testDir;
var testOperAccount;
var testOperDir;

test('setup: test accounts', function (t) {
    helper.ensureTestAccounts(t, function (err, accounts) {
        t.ifError(err, 'no error loading/creating test accounts');
        testAccount = accounts.regular;
        t.ok(testAccount, 'have regular test account: ' + testAccount.login);
        testOperAccount = accounts.operator;
        t.ok(testOperAccount,
            'have operator test account: ' + testOperAccount.login);
        t.end();
    });
});

test('setup: test dir', function (t) {
    client = helper.mantaClientFromAccountInfo(testAccount);
    operClient = helper.mantaClientFromAccountInfo(testOperAccount);
    storDir = '/' + testAccount.login + '/stor';
    stringClient = helper.createStringClient();
    var marker = uuidv4().split('-')[0];
    testDir = '/' + testAccount.login + '/stor/test-dir-operator-' + marker;
    testOperDir = '/' + testOperAccount.login +
        '/stor/test-dir-operator-' + marker;

    client.mkdir(testDir, function (err) {
        t.ifError(err, 'no error making testDir:' + testDir);
        operClient.mkdir(testOperDir, function (operErr) {
            t.ifError(operErr, 'no error making testOperDir: ' + testOperDir);
            t.end();
        });
    });
});

// Test the operator-only 'sort=none' and 'skip_owner_check=true' query
// params to Manta ListDirectory. These tests do not use the node-manta client
// because it doesn't currently expose those operator-only query params.

test('operator can ls with sort=none', function (t) {
    var query = {
        sort: 'none'
    };

    helper.signReq(testOperAccount, function (signErr, authorization, date) {
        t.ifError(signErr);

        stringClient.get({
            path: storDir,
            headers: {
                authorization: authorization,
                date: date
            },
            query: query
        }, function (err) {
            t.ifError(err);
            t.end();
        });
    });
});


test('operator can ls with skip_owner_check=true', function (t) {
    var query = {
        skip_owner_check: 'true'
    };

    helper.signReq(testOperAccount, function (signErr, authorization, date) {
        t.ifError(signErr);

        stringClient.get({
            path: storDir,
            headers: {
                authorization: authorization,
                date: date
            },
            query: query
        }, function (err) {
            t.ifError(err);
            t.end();
        });
    });
});


test('operator can ls with sort and no owner check', function (t) {
    var query = {
        sort: 'none',
        skip_owner_check: 'true'
    };

    helper.signReq(testOperAccount, function (signErr, authorization, date) {
        t.ifError(signErr);

        stringClient.get({
            path: storDir,
            headers: {
                authorization: authorization,
                date: date
            },
            query: query
        }, function (err) {
            t.ifError(err);
            t.end();
        });
    });
});


test('regular account cannot ls with sort=none', function (t) {
    var query = {
        sort: 'none'
    };

    helper.signReq(testAccount, function (signErr, authorization, date) {
        t.ifError(signErr);

        stringClient.get({
            path: storDir,
            headers: {
                authorization: authorization,
                date: date
            },
            query: query
        }, function (err) {
            t.ok(err, 'expected error');
            t.equal(err.statusCode, 403, 'expected statuscode of 403');

            // We expect the error response body to be JSON something like:
            //     {"code":"QueryParameterForbidden",
            //      "message":"Use of these query parameters is
            //          restricted to operators: sort=none"}
            // The StringClient we are using doesn't parse this.
            try {
                var errBody = JSON.parse(err.body);
                t.equal(errBody.code, 'QueryParameterForbidden');
            } catch (parseErr) {
                t.ok(false, 'expected GET body to be JSON error info, ' +
                    'got: ' + err.body);
            }

            t.end();
        });
    });
});

test('regular account cannot ls with skip_owner_check=true', function (t) {
    var query = {
        skip_owner_check: 'true'
    };

    helper.signReq(testAccount, function (signErr, authorization, date) {
        t.ifError(signErr);

        stringClient.get({
            path: storDir,
            headers: {
                authorization: authorization,
                date: date
            },
            query: query
        }, function (err) {
            t.ok(err, 'expected error');
            t.equal(err.statusCode, 403, 'expected statuscode of 403');

            // We expect the error response body to be JSON something like:
            //     {"code":"QueryParameterForbidden",
            //      "message":"Use of these query parameters is
            //          restricted to operators: sort=none"}
            // The StringClient we are using doesn't parse this.
            try {
                var errBody = JSON.parse(err.body);
                t.equal(errBody.code, 'QueryParameterForbidden');
            } catch (parseErr) {
                t.ok(false, 'expected GET body to be JSON error info, ' +
                    'got: ' + err.body);
            }

            t.end();
        });
    });
});

test('regular account cannot ls with no sort and no owner check', function (t) {
    var query = {
        sort: 'none',
        skip_owner_check: 'true'
    };

    helper.signReq(testAccount, function (signErr, authz, date) {
        t.ifError(signErr);

        stringClient.get({
            path: storDir,
            headers: {
                authorization: authz,
                date: date
            },
            query: query
        }, function (err) {
            t.ok(err, 'expected error');
            t.equal(err.statusCode, 403, 'expected statuscode of 403');

            // We expect the error response body to be JSON something like:
            //     {"code":"QueryParameterForbidden",
            //      "message":"Use of these query parameters is
            //          restricted to operators: sort=none"}
            // The StringClient we are using doesn't parse this.
            try {
                var errBody = JSON.parse(err.body);
                t.equal(errBody.code, 'QueryParameterForbidden');
            } catch (parseErr) {
                t.ok(false, 'expected GET body to be JSON error info, ' +
                    'got: ' + err.body);
            }

            t.end();
        });
    });
});


// Test that Manta ListDirectory still returns correct list of a directory
// even with the operator-only params (sort=none and skip_owner_check=true).
test('ls with operator-only params works', function (t) {
    var subdir = testOperDir + '/ls-with-operator-only-params-works';
    var dirNames = [];

    // Check that ListDirectory by the operator works properly with the given
    // operator-only query params.
    function checkLs(queryParams, cb) {
        assert.object(queryParams, 'queryParams');

        helper.signReq(testOperAccount, function (signErr, authz, date) {
            t.ifError(signErr);

            stringClient.get({
                path: subdir,
                headers: {
                    authorization: authz,
                    date: date
                },
                query: queryParams
            }, function (lsErr, req, res, body) {
                t.ifError(lsErr,
                    'expected no error from ListDirectory with query: ' +
                    JSON.stringify(queryParams));

                if (!lsErr) {
                    // Parse the ListDirectory response, entries like this:
                    //    {"name":"dir-0","type":"directory","mtime":"202..."}
                    // down to a sorted list of "name"s.
                    var listedNames = body.split(/\n/g)
                        .filter(function (line) { return line.trim(); })
                        .map(function (line) { return JSON.parse(line); })
                        .map(function (dirent) { return dirent.name; })
                        .sort();

                    t.deepEqual(listedNames, dirNames,
                        'expected to list ' + JSON.stringify(dirNames) +
                        ', got: ' + JSON.stringify(listedNames));
                }
                cb();
            });
        });
    }

    vasync.pipeline({funcs: [
        function setupSubdir(_, next) {
            operClient.mkdir(subdir, next);
        },
        function createSomeDirs(_, next) {
            for (var i = 0; i < 5; i++) {
                dirNames.push('dir-' + i);
            }

            vasync.forEachParallel({
                inputs: dirNames,
                func: function makeOne(dirName, nextDir) {
                    operClient.mkdir(subdir + '/' + dirName, nextDir);
                }
            }, function (err) {
                t.comment('setup ' + subdir + ' with a number of subdirs');
                next(err);
            });
        },
        function checkLsWithSort(_, next) {
            checkLs({sort: 'none'}, next);
        },
        function checkLsWithSkipOwnerCheck(_, next) {
            checkLs({skip_owner_check: 'true'}, next);
        },
        function checkLsWithSortAndSkipOwnerCheck(_, next) {
            checkLs({sort: 'none', skip_owner_check: 'true'}, next);
        }
    ]}, function (err) {
        t.ifError(err, 'expected no error running the pipeline');
        t.end();
    });
});

test('teardown', function (t) {
    client.rmr(testDir, function onRm(err) {
        t.ifError(err, 'remove testDir: ' + testDir);
        operClient.rmr(testOperDir, function onOperRm(operErr) {
            t.ifError(operErr, 'remove testOperDir: ' + testOperDir);
            t.end();
        });
    });
});
