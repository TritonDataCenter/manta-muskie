/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

// Test operations on directories related to *operator-only* access.

var assert = require('assert-plus');
var MemoryStream = require('stream').PassThrough;
var test = require('tap').test;
var uuidv4 = require('uuid/v4');
var vasync = require('vasync');

var helper = require('../helper');



///--- Globals

var assertMantaRes = helper.assertMantaRes;



///--- Helpers

function writeObject(client, key, opts, cb) {
    assert.string(key, 'key');

    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }

    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';
    var size = Buffer.byteLength(text);

    var putOpts = {
        headers: opts.headers,
        size: size
    };

    client.put(key, stream, putOpts, cb);
    process.nextTick(stream.end.bind(stream, text));
}

function writeStreamingObject(client, key, opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }

    var stream = new MemoryStream();
    var text = 'The lazy brown fox \nsomething \nsomething foo';

    var putOpts = {
        headers: opts.headers
    };

    process.nextTick(stream.end.bind(stream, text));
    client.put(key, stream, putOpts, function (err, res) {
        if (err) {
            cb(err);
        } else if (res.statusCode != 204) {
            cb(new Error('unsuccessful object write'));
        } else {
            cb();
        }
    });
}

/*
 * Tests whether (or not) we are allowed to list the contents of a dir, given
 * these input arguments:
 *   - t: the test object - to be passed in from a wrapper function
 *   - isOperator: whether we want to perform the request as an operator or not
 *   - expectOk: whether we want the request to succeed or fail
 *   - path: the directory to list
 *   - params: an object containing the desired query parameters, of the form:
 *     {
 *        key1: "value1"
 *        key2: "value2"
 *        ...
 *     }
 *
 * This function does not verify that the request returns well-formed results.
 * See testListWithParams for that.
 *
 * Because some query parameters are not exposed through the node-manta client,
 * we use a lower-level JSON client to perform the request.
 */
function testParamsAllowed(t, isOperator, expectOk, path, params) {
    var queryParams = params || {};
    var client = helper.createJsonClient();

    var key;
    var user;
    if (isOperator) {
        key = helper.getOperatorPrivkey();
        user = helper.TEST_OPERATOR;
    } else {
        key = helper.getRegularPrivkey();
        user = process.env.MANTA_USER;
    }
    var keyId = helper.getKeyFingerprint(key);
    var signOpts = {
        key: key,
        keyId: keyId,
        user: user
    };

    // Perform the ls request, and check the response according to expectOk
    helper.signRequest(signOpts, function gotSignature(err, authz, date) {
        var opts = {
            headers: {
                authorization: authz,
                date: date
            },
            path: path,
            query: queryParams
        };
        client.get(opts, function (get_err, get_req, get_res) {
            if (expectOk) {
                t.ifError(get_err);
            } else {
                t.ok(get_err, 'expected error');
                t.equal(get_err.statusCode, 403, 'expected statuscode of 403');
                t.equal(get_err.restCode, 'QueryParameterForbidden');
            }
            t.end();
        });
    });
}

/*
 * Verifies that we get well-formed results back when listing the contents of a
 * dir with the given query parameters. Arguments:
 *   - t: the test object - to be passed in from a wrapper function
 *   - params: an object containing the desired query parameters, of the form:
 *     {
 *        key1: "value1"
 *        key2: "value2"
 *        ...
 *     }
 *
 * This function ensures that the request will be permitted by always performing
 * the request as an operator, and thus does not test access control. See
 * testParamsAllowed for that.
 *
 * Because some query parameters are not exposed through the node-manta client,
 * we use a lower-level JSON client to perform the request.
 */
function testListWithParams(t, params) {
    var self = this;
    var queryParams = params || {};
    var client = helper.createJsonClient();

    var key = helper.getOperatorPrivkey();
    var keyId = helper.getKeyFingerprint(key);
    var user = helper.TEST_OPERATOR;
    var signOpts = {
        key: key,
        keyId: keyId,
        user: user
    };

    // Generate some subdirectory names we can read back
    var subdirs = [];
    var count = 5;
    var i;
    for (i = 0; i < count; i++) {
        subdirs.push(testDir + '/' + uuidv4());
    }
    subdirs = subdirs.sort();


    // Make the subdirectories
    vasync.forEachParallel({
        func: mkdir,
        inputs: subdirs
    }, function subdirsCreated(mkdir_err, results) {
        t.ifError(mkdir_err);

        // Read the subdirectories back with the specified query params
        helper.signRequest(signOpts, function gotSig(sig_err, authz, date) {
            t.ifError(sig_err);
            var opts = {
                headers: {
                    authorization: authz,
                    date: date
                },
                path: testDir,
                query: queryParams
            };
            client.get(opts, function (get_err, get_req, get_res) {
                t.ifError(get_err);

                // Parse the response body into a list of directory names
                var jsonStrings = get_res.body.split('\n').filter(isNotEmpty);
                var names = [];
                jsonStrings.forEach(function appendName(s) {
                    t.doesNotThrow(parseAndAppend.bind(null, s, names));
                });
                names.sort();

                // Verify that we got back all of the directories we created
                t.deepEqual(subdirs, names.map(prependDir));
                t.end();
            });
        });
    });

    // helper functions

    function mkdir(path, cb) {
        self.operatorClient.mkdir(path, function madeDir(err, res) {
            t.ifError(err);
            t.ok(res);
            assertMantaRes(t, res, 204);
            cb(err);
        });
    }

    function isNotEmpty(str) {
        return (str !== '');
    }

    function parseAndAppend(str, list) {
        list.push(JSON.parse(str).name);
    }

    function prependDir(name) {
        return (testDir + '/' + name);
    }
}


///--- Tests

var client;
var operClient;
var storDir;
var stringClient;
var testAccount;
var testOperAccount;

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
    var marker = uuidv4().split('-')[0]
    testDir = '/' + testAccount.login + '/stor/test-dir-operator-' + marker;
    testOperDir = '/' + testOperAccount.login +
        '/stor/test-dir-operator-' + marker;

    client.mkdir(testDir, function (err) {
        t.ifError(err, 'no error making testDir:' + testDir);
        operClient.mkdir(testOperDir, function (err) {
            t.ifError(err, 'no error making testOperDir: ' + testOperDir);
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
                    'got: ' + getResult.body);
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
                    'got: ' + getResult.body);
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
                    'got: ' + getResult.body);
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
                        .filter(line => line.trim())
                        .map(line => JSON.parse(line))
                        .map(dirent => dirent.name)
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
                t.comment(`setup ${subdir} with a number of subdirs`);
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
        },
    ]}, function (err) {
        t.ifError(err, 'expected no error running the pipeline');
        t.end();
    });
});

//test('ls with no sort returns accurate list of results', function (t) {
//    testListWithParams.bind(this)(t, {
//        sort: 'none'
//    });
//});

//test('ls with no owner check returns accurate list of results', function (t) {
//    testListWithParams.bind(this)(t, {
//        skip_owner_check: 'true'
//    });
//});

//test('ls with no sort and no owner check returns accurate list of results',
//    function (t) {
//    testListWithParams.bind(this)(t, {
//        sort: 'none',
//        skip_owner_check: 'true'
//    });
//});


test('teardown', function (t) {
    client.rmr(testDir, function onRm(err) {
        t.ifError(err, 'remove testDir: ' + testDir);
        operClient.rmr(testOperDir, function onRm(err) {
            t.ifError(err, 'remove testOperDir: ' + testOperDir);
            t.end();
        });
    });
});
