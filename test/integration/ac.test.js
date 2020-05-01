/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

// Test muskie access control (AC), aka RBAC.

var util = require('util');

var assert = require('assert-plus');
var MemoryStream = require('stream').PassThrough;
var once = require('once');
var test = require('tap').test;
var uuidv4 = require('uuid/v4');
var vasync = require('vasync');

var helper = require('../helper');


///--- Helpers

function writeObject(opts, cb) {
    assert.object(opts.client, 'opts.client');
    assert.string(opts.path, 'opts.path');
    assert.optionalObject(opts.headers, 'opts.headers');
    assert.func(cb, 'cb');

    var content = 'Hello, world.\n';
    var input = new MemoryStream();
    var writeOpts = {
        type: 'text/plain',
        size: Buffer.byteLength(content)
    };
    if (opts.headers) {
        writeOpts.headers = opts.headers;
    }

    var output = opts.client.createWriteStream(opts.path, writeOpts);
    output.once('close', cb.bind(null, null));
    output.once('error', cb);

    input.pipe(output);
    input.end(content);
}

// 1. Write an object with a role-tag (typically by the account client).
// 2. Sign the object, optionally with a role (typically by the subuser client).
// 3. Get the signed URL unauthed.
// ... then return the result of #3.
//
// This called back with `function (err, getResult)` where `err` is
// some error in writing or signing, and `getResult` is:
//      {
//          signedUrl: ...
//          err: ...
//          req: ...
//          res: ...
//          body: ...
//      }
// from the HTTP request of the signed URL.
function accountWriteSubuserSignAnonGet(opts, cb) {
    assert.object(opts.t, 'opts.t');
    assert.string(opts.path, 'opts.path');
    assert.object(opts.writeClient, 'opts.writeClient');
    assert.string(opts.writeRoleTag, 'opts.writeRoleTag');
    assert.object(opts.signClient, 'opts.signClient');
    assert.optionalString(opts.signRole, 'opts.signRole');
    assert.optionalObject(opts.getHeaders, 'opts.getHeaders');

    var signedUrl;
    var t = opts.t;

    vasync.pipeline({funcs: [
        function writeIt(_, next) {
            writeObject({
                client: opts.writeClient,
                path: opts.path,
                headers: {
                    'role-tag': opts.writeRoleTag
                }
            }, function (err) {
                t.comment(`wrote "${opts.path}" role-tag=${opts.writeRoleTag}`);
                next(err);
            });
        },
        function signIt(_, next) {
            var signOpts = {
                path: opts.path
            };
            if (opts.signRole) {
                signOpts.role = [opts.signRole];
            }
            opts.signClient.signURL(signOpts, function (err, s) {
                signedUrl = s;
                t.comment(`signed URL: ${signedUrl}`);
                next(err);
            });
        }
    ]}, function (err) {
        if (err) {
            cb(err);
        } else {
            var stringClient = helper.createStringClient();
            var getOpts = {
                path: signedUrl
            };
            if (opts.getHeaders) {
                getOpts.headers = opts.getHeaders
            }
            t.comment(`GETing signed URL: getOpts=${JSON.stringify(getOpts)}`);
            stringClient.get(getOpts, function (err, req, res, body) {
                cb(null, {
                    signedUrl: signedUrl,
                    err: err,
                    req: req,
                    res: res,
                    body: body
                });
            });
        }
    });
}


///--- Tests

test('access control', function (suite) {
    var client;
    var operClient;
    var subuserClient;
    var testAccount;
    var testDir;
    var testOperAccount;
    var testOperDir;

    suite.test('setup: test accounts', function (t) {
        helper.ensureTestAccounts(t, function (err, accounts) {
            t.ifError(err, 'no error loading/creating test accounts');
            testAccount = accounts.regular;
            testOperAccount = accounts.operator;
            t.ok(testAccount, 'have regular test account: ' +
                testAccount.login);
            t.ok(testOperAccount, 'have operator test account: ' +
                testOperAccount.login);
            t.end();
        });
    });

    suite.test('setup: test dir', function (t) {
        client = helper.mantaClientFromAccountInfo(testAccount);
        subuserClient = helper.mantaClientFromSubuserInfo(testAccount,
            'muskietest_subuser');
        operClient = helper.mantaClientFromAccountInfo(testOperAccount);
        var marker = uuidv4().split('-')[0]
        testDir = '/' + testAccount.login +
            '/stor/test-ac-dir-' + marker;
        testOperDir = '/' + testOperAccount.login +
            '/stor/test-ac-dir-' + marker;

        client.mkdir(testDir, function (err) {
            t.ifError(err, 'no error making testDir:' + testDir);
            operClient.mkdir(testOperDir, function (err) {
                t.ifError(err, 'no error making testOperDir: ' + testOperDir);
                t.end();
            });
        });
    });


    suite.test('get obj with default role', function (t) {
        var path = `${testDir}/obj-with-default-role`;

        writeObject({
            client: client,
            path: path,
            headers: {
                'role-tag': 'muskietest_role_default'
            }
        }, function (writeErr) {
            t.ifError(writeErr);
            if (writeErr) {
                t.end();
                return;
            }

            subuserClient.get(path, function (getErr) {
                t.ifError(getErr);
                t.end();
            });
        });
    });

    suite.test('get obj without needed role', function (t) {
        var path = `${testDir}/obj-without-needed-role`;
        writeObject({
            client: client,
            path: path,
            headers: {
                'role-tag': 'muskietest_role_limit'
            }
        }, function (writeErr) {
            t.ifError(writeErr);
            if (writeErr) {
                t.end();
                return;
            }

            subuserClient.get(path, function (getErr) {
                t.ok(getErr);
                if (getErr) {
                    t.equal(getErr.name, 'NoMatchingRoleTagError');
                }
                t.end();
            });
        });
    });

    suite.test('get obj using needed role', function (t) {
        var path = `${testDir}/obj-with-needed-role`;
        writeObject({
            client: client,
            path: path,
            headers: {
                'role-tag': 'muskietest_role_limit'
            }
        }, function (writeErr) {
            t.ifError(writeErr);
            if (writeErr) {
                t.end();
                return;
            }

            subuserClient.get(path, {
                headers: {
                    role: 'muskietest_role_limit'
                }
            }, function (getErr) {
                t.ifError(getErr);
                t.end();
            });
        });
    });

    suite.test('get obj using multiple roles', function (t) {
        var path = `${testDir}/obj-with-using-multiple-roles`;
        writeObject({
            client: client,
            path: path,
            headers: {
                'role-tag': 'muskietest_role_limit'
            }
        }, function (writeErr) {
            t.ifError(writeErr);
            if (writeErr) {
                t.end();
                return;
            }

            subuserClient.get(path, {
                headers: {
                    role: 'muskietest_role_default,muskietest_role_limit'
                }
            }, function (getErr) {
                t.ifError(getErr);
                t.end();
            });
        });
    });

    suite.test('get obj using wrong role', function (t) {
        var path = `${testDir}/obj-with-using-wrong-role`;
        writeObject({
            client: client,
            path: path,
            headers: {
                'role-tag': 'muskietest_role_default'
            }
        }, function (writeErr) {
            t.ifError(writeErr);
            if (writeErr) {
                t.end();
                return;
            }

            subuserClient.get(path, {
                headers: {
                    role: 'muskietest_role_limit'
                }
            }, function (getErr) {
                t.ok(getErr);
                if (getErr) {
                    t.equal(getErr.name, 'NoMatchingRoleTagError');
                }
                t.end();
            });
        });
    });

    suite.test('get obj using "*" roles', function (t) {
        var path = `${testDir}/obj-with-using-star-roles`;
        writeObject({
            client: client,
            path: path,
            headers: {
                'role-tag': 'muskietest_role_limit'
            }
        }, function (writeErr) {
            t.ifError(writeErr);
            if (writeErr) {
                t.end();
                return;
            }

            subuserClient.get(path, {
                headers: {
                    role: '*'
                }
            }, function (getErr) {
                t.ifError(getErr);
                t.end();
            });
        });
    });


    suite.test('get obj using bad role', function (t) {
        var path = `${testDir}/obj-with-using-bad-role`;
        writeObject({
            client: client,
            path: path,
            headers: {
                'role-tag': 'muskietest_role_other'
            }
        }, function (writeErr) {
            t.ifError(writeErr);
            if (writeErr) {
                t.end();
                return;
            }

            subuserClient.get(path, {
                headers: {
                    role: 'muskietest_role_other'
                }
            }, function (getErr) {
                t.ok(getErr);
                if (getErr) {
                    t.equal(getErr.name, 'InvalidRoleError');
                }
                t.end();
            });
        });
    });

    suite.test('get dir using bad role, cross-account', function (t) {
        var path = `/${testOperAccount.login}/stor`;
        subuserClient.get(path, {
            headers: {
                role: 'muskietest_role_other'
            }
        }, function (getErr) {
            t.ok(getErr);
            if (getErr) {
                t.equal(getErr.name, 'InvalidRoleError');
            }
            t.end();
        });
    });

    suite.test('mchmod/chattr an obj', function (t) {
        var path = `${testDir}/obj-to-mchmod`;
        writeObject({
            client: client,
            path: path,
            headers: {
                'role-tag': 'muskietest_role_write'
            }
        }, function (writeErr) {
            t.ifError(writeErr);
            if (writeErr) {
                t.end();
                return;
            }

            subuserClient.chattr(path, {
                headers: {
                    role: 'muskietest_role_write',
                    'role-tag': 'muskietest_role_other'
                }
            }, function (chattrErr) {
                t.ifError(chattrErr);
                if (chattrErr) {
                    t.end();
                    return;
                }

                client.info(path, function (infoErr, info) {
                    t.ifError(infoErr);
                    t.equal(info.headers['role-tag'], 'muskietest_role_other');
                    t.end();
                });
            });
        });
    });

    suite.test('mchmod/chattr to a bad role fails', function (t) {
        var path = `${testDir}/obj-to-mchmod-to-bad-role`;
        writeObject({
            client: client,
            path: path,
            headers: {
                'role-tag': 'muskietest_role_write'
            }
        }, function (writeErr) {
            t.ifError(writeErr);
            if (writeErr) {
                t.end();
                return;
            }

            subuserClient.chattr(path, {
                headers: {
                    role: 'muskietest_role_write',
                    'role-tag': 'bogus_role'
                }
            }, function (chattrErr) {
                t.ok(chattrErr, 'expect error from chattr');
                if (chattrErr) {
                    t.equal(chattrErr.name, 'InvalidRoleTagError');
                }
                t.end();
            });
        });
    });


    suite.test('subuser create obj fails on parent dir check', function (t) {
        var path = `${testDir}/obj-for-subuser-to-create`;
        writeObject({
            client: subuserClient,
            path: path,
            headers: {
                'role-tag': 'muskietest_role_write'
            }
        }, function (writeErr) {
            t.ok(writeErr);
            if (writeErr) {
                t.equal(writeErr.name, 'NoMatchingRoleTagError');
            }
            t.end();
        });
    });

    suite.test('subuser create obj fails on parent dir check', function (t) {
        var path = `${testDir}/obj-for-subuser-to-create`;
        writeObject({
            client: subuserClient,
            path: path,
            headers: {
                'role-tag': 'muskietest_role_write'
            }
        }, function (writeErr) {
            t.ok(writeErr);
            if (writeErr) {
                t.equal(writeErr.name, 'NoMatchingRoleTagError');
            }
            t.end();
        });
    });

    suite.test('subuser create dir fails on parent dir check', function (t) {
        var path = `${testDir}/obj-for-subuser-to-create`;
        subuserClient.mkdir(path, {
            headers: {
                'role-tag': 'muskietest_role_write'
            }
        }, function (err) {
            t.ok(err);
            if (err) {
                t.equal(err.name, 'NoMatchingRoleTagError');
            }
            t.end();
        });
    });

    // Ideally, getting a nonexistent object should mean a check on the parent
    // directory to see if the user has read permissions on the directory.
    // However, since this requires an additional lookup, we're just returning
    // 404s for now.
    suite.test('get nonexistent object 404', function (t) {
        var path = `${testDir}/no-such-obj`;
        client.get(path, function (err) {
            t.ok(err);
            if (err) {
                t.equal(err.name, 'ResourceNotFoundError');
            }
            t.end();
        });
    });

    suite.test('signed URL uses default roles', function (t) {
        accountWriteSubuserSignAnonGet({
            t: t,
            path: `${testDir}/obj-to-sign`,
            writeClient: client,
            writeRoleTag: 'muskietest_role_default',
            signClient: subuserClient
        }, function (err, getResult) {
            t.ifError(err, 'expected no error writing or signing');
            if (!err) {
                t.ifError(getResult.err, 'expected no error GETing signed URL');
                t.equal(getResult.res.statusCode, 200);
                t.ok(getResult.body);
            }
            t.end();
        });
    });

    suite.test('signed URL ignores role headers', function (t) {
        accountWriteSubuserSignAnonGet({
            t: t,
            path: `${testDir}/obj-to-sign`,
            writeClient: client,
            writeRoleTag: 'muskietest_role_limit',
            signClient: subuserClient,
            getHeaders: {
                role: 'muskietest_role_limit'
            }
        }, function (err, getResult) {
            t.ifError(err, 'expected no error writing or signing');
            if (getResult) {
                t.ok(getResult.err, 'expected failure GETing signed URL');
                if (getResult.res) {
                    t.equal(getResult.res.statusCode, 403);
                }
                if (getResult.err) {
                    // We expect the string body of the error response to look
                    // like this:
                    //      {"code":"NoMatchingRoleTag",
                    //       "message":"None of your active roles ..."}
                    try {
                        var errBody = JSON.parse(getResult.body);
                        t.equal(errBody.code, 'NoMatchingRoleTag');
                    } catch (parseErr) {
                        t.ok(false, 'expected GET body to be JSON error, ' +
                            'got: ' + getResult.body);
                    }
                }
            }
            t.end();
        });
    });

    suite.test('signed URL with included role', function (t) {
        accountWriteSubuserSignAnonGet({
            t: t,
            path: `${testDir}/obj-to-sign`,
            writeClient: client,
            writeRoleTag: 'muskietest_role_limit',
            signClient: subuserClient,
            signRole: 'muskietest_role_limit'   // <--- the "included role"
        }, function (err, getResult) {
            t.ifError(err, 'expected no error writing or signing');
            if (!err) {
                t.ifError(getResult.err, 'expected no error GETing signed URL');
                t.equal(getResult.res.statusCode, 200);
                t.ok(getResult.body);
            }
            t.end();
        });
    });

    suite.test('signed URL with included wrong role', function (t) {
        accountWriteSubuserSignAnonGet({
            t: t,
            path: `${testDir}/obj-to-sign`,
            writeClient: client,
            writeRoleTag: 'muskietest_role_default',
            signClient: subuserClient,
            signRole: 'muskietest_role_limit'   // <--- wrong role
        }, function (err, getResult) {
            t.ifError(err, 'expected no error writing or signing');
            if (getResult) {
                t.ok(getResult.err, 'expected failure GETing signed URL');
                if (getResult.res) {
                    t.equal(getResult.res.statusCode, 403);
                }
                if (getResult.err) {
                    // We expect the string body of the error response to look
                    // like this:
                    //      {"code":"NoMatchingRoleTag",
                    //       "message":"None of your active roles ..."}
                    try {
                        var errBody = JSON.parse(getResult.body);
                        t.equal(errBody.code, 'NoMatchingRoleTag');
                    } catch (parseErr) {
                        t.ok(false, 'expected GET body to be JSON error, ' +
                            'got: ' + getResult.body);
                    }
                }
            }
            t.end();
        });
    });

    suite.test('signed URL with included invalid role', function (t) {
        accountWriteSubuserSignAnonGet({
            t: t,
            path: `${testDir}/obj-to-sign`,
            writeClient: client,
            writeRoleTag: 'muskietest_role_default',
            signClient: subuserClient,
            signRole: 'muskietest_role_bogus'   // <--- invalid role
        }, function (err, getResult) {
            t.ifError(err, 'expected no error writing or signing');
            if (getResult) {
                t.ok(getResult.err, 'expected failure GETing signed URL');
                if (getResult.res) {
                    t.equal(getResult.res.statusCode, 409);
                }
                if (getResult.err) {
                    // We expect the string body of the error response to look
                    // like this:
                    //      {"code":"InvalidRole",
                    //       "message":"Role \"...\" is invalid."}
                    try {
                        var errBody = JSON.parse(getResult.body);
                        t.equal(errBody.code, 'InvalidRole');
                    } catch (parseErr) {
                        t.ok(false, 'expected GET body to be JSON error, ' +
                            'got: ' + getResult.body);
                    }
                }
            }
            t.end();
        });
    });

    // Tests for scenarios around rules with the "*"" or "all" aperture
    // resource (support added with MANTA-3962).
    suite.test('all-resource rules, untagged', function (t) {
        var path = `${testDir}/obj-for-star-rules-untagged`;
        var role = 'muskietest_role_star';

        vasync.pipeline({funcs: [
            // First, create a test object, with no role tags.
            function createTestObjWithNoRoleTags(_, next) {
                writeObject({
                    client: client,
                    path: path
                }, next);
            },

            // This should not work: we haven't activated the role and we have
            // no default roles that are tagged on the object, so we have no
            // right to read it.
            function subuserInfoShouldFail(_, next) {
                subuserClient.info(path, function (err) {
                    t.ok(err, 'expected error on subuser info without role');
                    next();
                });
            },

            // This should not work either: the role has a rule "Can putobject"
            // but without the * this doesn't apply to all objects, only role-
            // tagged ones, and this object has no role-tag.
            function subuserWriteShouldFail(_, next) {
                writeObject({
                    client: subuserClient,
                    path: path,
                    headers: {
                        'role-tag': role
                    }
                }, function (err) {
                    t.ok(err, 'expected error on subuser write, even with role');
                    next();
                });
            },

            // This should work, though: the "Can getobject *" rule kicks
            // in, even though this object isn't tagged (thanks to the *).
            function subuserInfoWithStarRoleShouldWork(_, next) {
                subuserClient.info(path, {
                    headers: {
                        role: role
                    }
                }, function (err, info) {
                    t.ifError(err, 'expected subuser info with * role to succeed');
                    t.strictEqual(info.headers['role-tag'], undefined);
                    next();
                });
            }
        ]}, function finish(err) {
            t.ifError(err);
            t.end();
        });
    });


    suite.test('all-resource rules, tagged', function (t) {
        var path = `${testDir}/obj-for-star-rules-tagged`;
        var role = 'muskietest_role_star';

        vasync.pipeline({funcs: [
            // First, create a test object, this time tagged to the role.
            function createTestObj(_, next) {
                writeObject({
                    client: client,
                    path: path,
                    headers: {
                        'role-tag': role
                    }
                }, next);
            },

            // We should be able to write it, since it's role-tagged so the
            // "Can putobject" rule applies.
            function subuserWriteWithRoleShouldWork(_, next) {
                writeObject({
                    client: subuserClient,
                    path: path,
                    headers: {
                        role: role,
                        'role-tag': role
                    }
                }, function (err) {
                    t.ifError(err, 'expected subuser write with role to work');
                    next();
                });
            },

            // And we should also be able to read it thanks to the
            // "Can getobject *" rule.
            function subuserInfoWithRoleShouldWork(_, next) {
                subuserClient.info(path, {
                    headers: {
                        role: role
                    }
                }, function (err, info) {
                    t.ifError(err, 'expected subuser info with role to work');
                    t.equal(info.headers['role-tag'], role);
                    t.end();
                });
            }
        ]}, function finish(err) {
            t.ifError(err);
            t.end();
        });
    });

    // Tests for scenarios around rules with explicit resource strings (support
    // added with MANTA-4284).
    suite.test('explicit resource rules', function (t) {
        var path = `${testDir}/globbity-obj-1`;
        var role = 'muskietest_role_glob';

        vasync.pipeline({funcs: [
            // First, create a test object, with no role tags.
            function createTestObjWithNoRoleTags(_, next) {
                writeObject({
                    client: client,
                    path: path
                }, next);
            },

            // This should not work: we haven't activated the role and we have
            // no default roles that are tagged on the object, so we have no
            // right to read it.
            function subuserInfoShouldFail(_, next) {
                subuserClient.info(path, function (err) {
                    t.ok(err, 'expected error on subuser info without role');
                    next();
                });
            },

            // This should work, though: the "Can getobject /..." rule kicks
            // in, even though this object isn't tagged.
            function subuserInfoWithGlobRoleShouldWork(_, next) {
                subuserClient.info(path, {
                    headers: {
                        role: role
                    }
                }, function (err, info) {
                    t.ifError(err, 'expected subuser info with glob role to work');
                    t.strictEqual(info.headers['role-tag'], undefined);
                    next();
                });
            }
        ]}, function finish(err) {
            t.ifError(err);
            t.end();
        });
    });

    suite.test('explicit resource rules, denied', function (t) {
        var path = `${testDir}/does-not-match-globbity-pattern`;
        var role = 'muskietest_role_glob';

        vasync.pipeline({funcs: [
            // First, create a test object, with no role tags.
            function createTestObjWithNoRoleTags(_, next) {
                writeObject({
                    client: client,
                    path: path
                }, next);
            },

            // This should not work: we haven't activated the role and we have
            // no default roles that are tagged on the object, so we have no
            // right to read it.
            function subuserInfoShouldFail(_, next) {
                subuserClient.info(path, function (err) {
                    t.ok(err, 'expected error on subuser info without role');
                    next();
                });
            },

            // This should not work, either: the rule with the explicit
            // resource on muskietest_role_glob does not match the path.
            function subuserInfoWithGlobRoleShouldFail(_, next) {
                subuserClient.info(path, {
                    headers: {
                        role: role
                    }
                }, function (err, info) {
                    t.ok(err, 'expected subuser info with glob role to fail');
                    t.equal(err.name, 'ForbiddenError');
                    next();
                });
            }
        ]}, function finish(err) {
            t.ifError(err);
            t.end();
        });
    });


    suite.test('cross-account role access, denied', function (t) {
        var path = testOperDir;
        client.info(path, {
            headers: {
                'role': 'muskietest_role_xacct'
            }
        }, function (err, info) {
            t.ok(err);
            // Note that currently Manta will respond 403 Forbidden for an
            // *existing* path, and 404 Not Found for a non-existant path.
            // We *happen* to know that `testOperDir` already exists from the
            // earlier setup steps in this test file.
            t.equal(err.name, 'ForbiddenError');

            client.info(path, function (err2, info2) {
                t.ok(err2);
                t.equal(err2.name, 'ForbiddenError');
                t.end();
            });
        });
    });

    suite.test('cross-account role access', function (t) {
        var path = `${testOperDir}/obj-for-xacct-access`;
        vasync.pipeline({funcs: [
            function writeObjWithoutRoleTag(_, next) {
                writeObject({
                    client: operClient,
                    path: path
                }, next);
            },

            function chattrToAddXacctRoleTag(_, next) {
                operClient.chattr(path, {
                    headers: {
                        'role-tag': 'muskietest_role_xacct'
                    }
                }, next);
            },

            function clientInfoShouldWork(_, next) {
                client.info(path, {
                    headers: {
                        role: 'muskietest_role_xacct'
                    }
                }, function (err, info) {
                    t.ifError(err, 'expected xacct client.info to work');
                    t.equal(info.headers['role-tag'], 'muskietest_role_xacct');
                    next();
                });
            }
        ]}, function finish(err) {
            t.ifError(err);
            t.end();
        });
    });

    suite.test('created object gets parent dir roles', function (t) {
        var dir = `${testDir}/dir-with-roles`;
        var path = `${dir}/obj-inside-dir-with-roles`;

        vasync.pipeline({funcs: [
            function makeDir(_, next) {
                client.mkdir(dir, next);
            },

            function addRoleTagToDir(_, next) {
                client.chattr(dir, {
                    headers: {
                        'role-tag': 'muskietest_role_write'
                    }
                }, next);
            },

            function subuserWriteObjUnderThatDir(_, next) {
                writeObject({
                    client: subuserClient,
                    path: path,
                    headers: {
                        role: 'muskietest_role_write'
                    }
                }, next);
            },

            function writtenObjShouldHaveInheritedRoleTag(_, next) {
                client.info(path, function (err, info) {
                    t.ifError(err, 'expected client.info to work');
                    if (!err) {
                        var roleTags = info.headers['role-tag'].split(/,/);
                        t.ok(roleTags.indexOf('muskietest_role_write') !== -1,
                            'have "muskietest_role_write" role-tag from parent dir');
                        next();
                    }
                })
            }
        ]}, function finish(err) {
            t.ifError(err);
            t.end();
        });
    });


    // TODOs since 2014, at least:
    // TODO assets OK
    // TODO conditions - overwrite
    // TODO conditions - day/date/time
    // TODO conditions - sourceip
    // TODO conditions - user-agent


    suite.test('teardown', function (t) {
        client.rmr(testDir, function onRm(err) {
            t.ifError(err, 'remove testDir: ' + testDir);
            operClient.rmr(testOperDir, function onRm(err) {
                t.ifError(err, 'remove testOperDir: ' + testOperDir);
                t.end();
            });
        });
    });

    suite.end();
});
