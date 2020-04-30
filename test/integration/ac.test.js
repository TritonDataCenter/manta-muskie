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
var test = require('@smaller/tap').test;
var uuidv4 = require('uuid/v4');

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


///--- Tests

test('access control', function (suite) {
    var client;
    var testDir;
    // XXX
    //var jsonClient = helper.createJsonClient();
    //var stringClient;
    var subuserClient;
    var testAccount;
    var testOperatorAccount;

    suite.test('setup: test accounts', function (t) {
        helper.ensureTestAccounts(t, function (err, accounts) {
            t.ifError(err, 'no error loading/creating test accounts');
            testAccount = accounts.regular;
            testOperatorAccount = accounts.operator;
            t.ok(testAccount, 'have regular test account: ' +
                testAccount.login);
            t.ok(testOperatorAccount, 'have operator test account: ' +
                testOperatorAccount.login);
            t.end();
        });
    });

    suite.test('setup: test subusers', function (t) {
        helper.ensureRbacSettings({
            t: t,
            account: testAccount,
            subusers: [
                {
                    login: 'muskietest_subuser',
                    password: 'secret123',
                    email: 'muskietest_subuser@localhost'
                }
            ],
            policies: [
                {
                    name: 'muskietest_policy_read',
                    rules: [
                        'Can getobject'
                    ]
                },
                {
                    name: 'muskietest_policy_write',
                    rules: [
                        'Can putobject',
                        'Can putdirectory'
                    ]
                },
                {
                    name: 'muskietest_policy_star',
                    rules: [
                        'Can getobject *',
                        'Can putobject'
                    ]
                },
                {
                    name: 'muskietest_policy_glob',
                    rules: [
                        // XXX test-ac-* ?
                        'Can getobject /' + testAccount.login + '/stor/muskietest_glob*'
                    ]
                }
            ],
            roles: [
                {
                    name: 'muskietest_role_default',
                    members: [ 'muskietest_subuser' ],
                    default_members: [ 'muskietest_subuser' ],
                    policies: [ 'muskietest_policy_read' ]
                },
                {
                    name: 'muskietest_role_limit',
                    members: [ 'muskietest_subuser' ],
                    policies: [ 'muskietest_policy_read' ]
                },
                {
                    name: 'muskietest_role_other',
                    policies: ['muskietest_policy_read']
                },
                {
                    name: 'muskietest_role_write',
                    members: [ 'muskietest_subuser' ],
                    policies: [ 'muskietest_policy_write' ]
                },
                {
                    name: 'muskietest_role_star',
                    members: [ 'muskietest_subuser' ],
                    policies: [ 'muskietest_policy_star' ]
                },
                {
                    name: 'muskietest_role_glob',
                    members: [ 'muskietest_subuser' ],
                    policies: [ 'muskietest_policy_glob' ]
                },
                {
                    name: 'muskietest_role_all',
                    members: [ 'muskietest_subuser' ],
                    policies: [
                        'muskietest_policy_read',
                        'muskietest_policy_write'
                    ]
                }
            ]
        }, function (err) {
            t.ifError(err, 'no error setting up RBAC on account ' +
                testAccount.login);
            t.end();
        });
    });

    suite.test('setup: test dir', function (t) {
        client = helper.mantaClientFromAccountInfo(testAccount);
        // XXX
        //stringClient = helper.createStringClient();
        subuserClient = helper.mantaClientFromSubuserInfo(testAccount,
            'muskietest_subuser');
        testDir = '/' + testAccount.login +
            '/stor/test-ac-dir-' + uuidv4().split('-')[0];

        client.mkdir(testDir, function (err) {
            t.ifError(err, 'no error making test dir ' + testDir);
            t.end();
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

// XXX START HERE

    //
    //suite.test('assume bad role - xacct', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor', self.operClient.user);
    //    self.client.get(path, {
    //        headers: {
    //            'role': 'muskietest_role_other'
    //        }
    //    }, function (err2) {
    //        if (!err2) {
    //            t.fail(err2, 'error expected');
    //            t.end();
    //            return;
    //        }
    //        t.equal(err2.name, 'InvalidRoleError');
    //        t.end();
    //    });
    //});
    //
    //suite.test('mchmod', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    //    var roles = 'muskietest_role_write';
    //    writeObject(self.client, path, roles, function (err) {
    //        if (err) {
    //            t.fail(err);
    //            t.end();
    //            return;
    //        }
    //
    //        self.paths.push(path);
    //        self.userClient.chattr(path, {
    //            headers: {
    //                'role': 'muskietest_role_write',
    //                'role-tag': 'muskietest_role_other'
    //            }
    //        }, function (err2) {
    //            if (err2) {
    //                t.fail(err2);
    //                t.end();
    //                return;
    //            }
    //
    //            self.client.info(path, function (err3, info) {
    //                if (err3) {
    //                    t.fail(err3);
    //                    t.end();
    //                    return;
    //                }
    //                t.equal(info.headers['role-tag'], 'muskietest_role_other');
    //                t.end();
    //            });
    //        });
    //    });
    //});
    //
    ///*
    // * Tests for scenarios around rules with the "*"" or "all" aperture
    // * resource (support added with MANTA-3962).
    // */
    //suite.test('all-resource rules (untagged)', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    //    var role = 'muskietest_role_star';
    //    /* First, create a test object, with no role tags. */
    //    writeObject(self.client, path, function (err) {
    //        if (err) {
    //            t.fail(err);
    //            t.end();
    //            return;
    //        }
    //        self.paths.push(path);
    //
    //        /*
    //         * This should not work: we haven't activated the role and we have
    //         * no default roles that are tagged on the object, so we have no
    //         * right to read it.
    //         */
    //        self.userClient.info(path, function (err2) {
    //            if (!err2) {
    //                t.fail('error expected');
    //                t.end();
    //                return;
    //            }
    //
    //            /*
    //             * This should not work either: the role has a rule "Can putobject"
    //             * but without the * this doesn't apply to all objects, only role-
    //             * tagged ones, and this object has no role-tag.
    //             */
    //            writeObject(self.userClient, path, {
    //                'role': role
    //            }, function (err3) {
    //                if (!err3) {
    //                    t.fail('error expected');
    //                    t.end();
    //                    return;
    //                }
    //
    //                /*
    //                 * This should work, though: the "Can getobject *" rule kicks
    //                 * in, even though this object isn't tagged (thanks to the *).
    //                 */
    //                self.userClient.info(path, {
    //                    headers: {
    //                        'role': role
    //                    }
    //                }, function (err4, info) {
    //                    if (err4) {
    //                        t.fail(err4);
    //                        t.end();
    //                        return;
    //                    }
    //                    t.strictEqual(info.headers['role-tag'], undefined);
    //                    t.end();
    //                });
    //            });
    //        });
    //    });
    //});
    //
    //suite.test('all-resource rules (tagged)', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    //    var role = 'muskietest_role_star';
    //    /* First, create a test object, this time tagged to the role. */
    //    writeObject(self.client, path, role, function (err) {
    //        if (err) {
    //            t.fail(err);
    //            t.end();
    //            return;
    //        }
    //        self.paths.push(path);
    //
    //        /*
    //         * We should be able to write it, since it's role-tagged so the
    //         * "Can putobject" rule applies.
    //         */
    //        writeObject(self.userClient, path, {
    //            'role': role,
    //            'role-tag': role
    //        }, function (err2) {
    //            if (err2) {
    //                t.fail(err2);
    //                t.end();
    //                return;
    //            }
    //
    //            /*
    //             * And we should also be able to read it thanks to the
    //             * "Can getobject *" rule.
    //             */
    //            self.userClient.info(path, {
    //                headers: {
    //                    'role': role
    //                }
    //            }, function (err3, info) {
    //                if (err3) {
    //                    t.fail(err3);
    //                    t.end();
    //                    return;
    //                }
    //                t.equal(info.headers['role-tag'], role);
    //                t.end();
    //            });
    //        });
    //    });
    //});
    //
    ///*
    // * Tests for scenarios around rules with explicit resource strings (support
    // * added with MANTA-4284).
    // */
    //suite.test('explicit resource rules', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_glob_1', self.client.user);
    //    var role = 'muskietest_role_glob';
    //    /* First, create a test object, with no role tags. */
    //    writeObject(self.client, path, function (err) {
    //        if (err) {
    //            t.fail(err);
    //            t.end();
    //            return;
    //        }
    //        self.paths.push(path);
    //
    //        /*
    //         * This should not work: we haven't activated the role and we have
    //         * no default roles that are tagged on the object, so we have no
    //         * right to read it.
    //         */
    //        self.userClient.info(path, function (err2) {
    //            if (!err2) {
    //                t.fail('error expected');
    //                t.end();
    //                return;
    //            }
    //
    //            /*
    //             * This should work, though: the "Can getobject /..." rule kicks
    //             * in, even though this object isn't tagged.
    //             */
    //            self.userClient.info(path, {
    //                headers: {
    //                    'role': role
    //                }
    //            }, function (err4, info) {
    //                if (err4) {
    //                    t.fail(err4);
    //                    t.end();
    //                    return;
    //                }
    //                t.strictEqual(info.headers['role-tag'], undefined);
    //                t.end();
    //            });
    //        });
    //    });
    //});
    //
    //suite.test('explicit resource rules (denied)', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_noglob', self.client.user);
    //    var role = 'muskietest_role_glob';
    //    /* First, create a test object, with no role tags. */
    //    writeObject(self.client, path, function (err) {
    //        if (err) {
    //            t.fail(err);
    //            t.end();
    //            return;
    //        }
    //        self.paths.push(path);
    //
    //        /*
    //         * This should not work: we haven't activated the role and we have
    //         * no default roles that are tagged on the object, so we have no
    //         * right to read it.
    //         */
    //        self.userClient.info(path, function (err2) {
    //            if (!err2) {
    //                t.fail('error expected');
    //                t.end();
    //                return;
    //            }
    //
    //            /*
    //             * This should not work, either: the rule with the explicit
    //             * resource on muskietest_role_glob does not match the path.
    //             */
    //            self.userClient.info(path, {
    //                headers: {
    //                    'role': role
    //                }
    //            }, function (err4, info) {
    //                if (!err4) {
    //                    t.fail('error expected');
    //                }
    //                t.equal(err4.name, 'ForbiddenError');
    //                t.end();
    //            });
    //        });
    //    });
    //});
    //
    //suite.test('cross-account role access (denied)', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor', self.operClient.user);
    //    self.client.info(path, {
    //        headers: {
    //            'role': 'muskietest_role_xacct'
    //        }
    //    }, function (err3, info) {
    //        if (!err3) {
    //            t.fail('error expected');
    //            t.end();
    //            return;
    //        }
    //        t.equal(err3.name, 'ForbiddenError');
    //
    //        self.client.info(path, function (err4, info2) {
    //            if (!err4) {
    //                t.fail('error expected');
    //                t.end();
    //                return;
    //            }
    //            t.equal(err4.name, 'ForbiddenError');
    //            t.end();
    //        });
    //    });
    //});
    //
    //suite.test('cross-account role access', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_obj', self.operClient.user);
    //    writeObject(self.operClient, path, function (err) {
    //        if (err) {
    //            t.fail(err);
    //            t.end();
    //            return;
    //        }
    //
    //        self.operPaths.push(path);
    //        self.operClient.chattr(path, {
    //            headers: {
    //                'role-tag': 'muskietest_role_xacct'
    //            }
    //        }, function (err2) {
    //            if (err2) {
    //                t.fail(err2);
    //                t.end();
    //                return;
    //            }
    //
    //            self.client.info(path, {
    //                headers: {
    //                    'role': 'muskietest_role_xacct'
    //                }
    //            }, function (err3, info) {
    //                if (err3) {
    //                    t.fail(err3);
    //                    t.end();
    //                    return;
    //                }
    //                t.equal(info.headers['role-tag'], 'muskietest_role_xacct');
    //                t.end();
    //            });
    //        });
    //    });
    //});
    //
    //suite.test('mchmod bad role', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    //    var roles = 'muskietest_role_write';
    //    writeObject(self.client, path, roles, function (err) {
    //        if (err) {
    //            t.fail(err);
    //            t.end();
    //            return;
    //        }
    //
    //        self.paths.push(path);
    //        self.userClient.chattr(path, {
    //            headers: {
    //                'role': 'muskietest_role_write',
    //                'role-tag': 'asdf'
    //            }
    //        }, function (err2) {
    //            if (!err2) {
    //                t.fail('error expected');
    //                t.end();
    //                return;
    //            }
    //            t.equal(err2.name, 'InvalidRoleTagError');
    //            t.end();
    //        });
    //    });
    //});
    //
    //
    //suite.test('created object gets roles', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    //    var dir = sprintf('/%s/stor', self.client.user);
    //    addTag(self.client, dir, 'muskietest_role_write', function (err) {
    //        if (err) {
    //            t.fail(err);
    //            t.end();
    //            return;
    //        }
    //        writeObject(self.userClient, path, {
    //            'role': 'muskietest_role_write'
    //        }, function (err2) {
    //            if (err2) {
    //                t.fail(err2);
    //                t.end();
    //                return;
    //            }
    //
    //            self.paths.push(path);
    //
    //            self.client.info(path, function (err3, info) {
    //                if (err3) {
    //                    t.fail(err3);
    //                    t.end();
    //                    return;
    //                }
    //
    //                /* JSSTYLED */
    //                var tags = info.headers['role-tag'].split(/\s*,\s*/);
    //                t.ok(tags.indexOf('muskietest_role_write') >= 0);
    //
    //                delTag(self.client, dir, 'muskietest_role_write',
    //                        function (err4) {
    //
    //                    if (err4) {
    //                        t.fail(err4);
    //                        t.end();
    //                        return;
    //                    }
    //                    t.end();
    //                });
    //            });
    //        });
    //    });
    //});
    //
    //
    //suite.test('create object parent directory check', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    //    writeObject(self.userClient, path, {
    //        'role': 'muskietest_role_write'
    //    }, function (err2) {
    //        if (!err2) {
    //            self.paths.push(path);
    //            t.fail('expected error');
    //            t.end();
    //            return;
    //        }
    //
    //        t.equal(err2.name, 'NoMatchingRoleTagError');
    //        t.end();
    //    });
    //});
    //
    //
    //suite.test('create object parent directory check', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    //    writeObject(self.userClient, path, {
    //        'role': 'muskietest_role_write'
    //    }, function (err) {
    //        if (!err) {
    //            self.paths.push(path);
    //            t.fail('expected error');
    //            t.end();
    //            return;
    //        }
    //
    //        t.equal(err.name, 'NoMatchingRoleTagError');
    //        t.end();
    //    });
    //});
    //
    //
    //suite.test('create directory parent directory check', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_dir', self.client.user);
    //    self.userClient.mkdir(path, {
    //        headers: {
    //            'role': 'muskietest_role_write'
    //        }
    //    }, function (err2) {
    //        if (!err2) {
    //            self.paths.push(path);
    //            t.fail('expected error');
    //            t.end();
    //            return;
    //        }
    //
    //        t.equal(err2.name, 'NoMatchingRoleTagError');
    //        t.end();
    //    });
    //});
    //
    //
    //// Ideally, getting a nonexistent object should mean a check on the parent
    //// directory to see if the user has read permissions on the directory. However,
    //// since this requires an additional lookup, we're just returning 404s for now.
    //suite.test('get nonexistent object 404', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_dir', self.client.user);
    //    self.client.get(path, function (err2) {
    //        if (!err2) {
    //            t.fail('error expected');
    //            t.end();
    //            return;
    //        }
    //        t.equal(err2.name, 'ResourceNotFoundError');
    //        t.end();
    //    });
    //});
    //
    //
    //suite.test('signed URL uses default roles', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    //    var roles = 'muskietest_role_default';
    //    var signed;
    //
    //    vasync.pipeline({funcs: [
    //        function write(_, cb) {
    //            writeObject(self.client, path, roles, cb);
    //        },
    //        function sign(_, cb) {
    //            helper.signUrl({
    //                path: path,
    //                client: self.userClient
    //            }, function (err, s) {
    //                if (err) {
    //                    cb(err);
    //                    return;
    //                }
    //                signed = s;
    //                cb();
    //            });
    //        },
    //        function get(_, cb) {
    //            self.jsonClient.get({
    //                path: signed
    //            }, function (err, req, res, obj) {
    //                if (err) {
    //                    t.fail(err);
    //                    cb(err);
    //                    return;
    //                }
    //                t.ok(obj);
    //                cb();
    //            });
    //        }
    //    ]}, function (err, results) {
    //        if (err) {
    //            t.fail(results.operations[results.ndone - 1]);
    //            t.end();
    //            return;
    //        }
    //
    //        t.end();
    //    });
    //});
    //
    //
    //suite.test('signed URL ignores role headers', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    //    var roles = 'muskietest_role_limit';
    //    var signed;
    //
    //    vasync.pipeline({funcs: [
    //        function write(_, cb) {
    //            writeObject(self.client, path, roles, cb);
    //        },
    //        function sign(_, cb) {
    //            helper.signUrl({
    //                path: path,
    //                client: self.userClient
    //            }, function (err, s) {
    //                if (err) {
    //                    cb(err);
    //                    return;
    //                }
    //                signed = s;
    //                cb();
    //            });
    //        },
    //        function get(_, cb) {
    //            self.jsonClient.get({
    //                path: signed,
    //                headers: {
    //                    role: 'muskietest_role_limit'
    //                }
    //            }, function (err) {
    //                if (!err) {
    //                    t.fail('expected error');
    //                    cb();
    //                    return;
    //                }
    //
    //                t.equal(err.name, 'NoMatchingRoleTagError');
    //                cb();
    //            });
    //        }
    //    ]}, function (err, results) {
    //        if (err) {
    //            t.fail(results.operations[results.ndone - 1]);
    //            t.end();
    //            return;
    //        }
    //
    //        t.end();
    //    });
    //});
    //
    //
    //suite.test('signed URL with included role', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    //    var roles = 'muskietest_role_limit';
    //    var signed;
    //
    //    vasync.pipeline({funcs: [
    //        function write(_, cb) {
    //            writeObject(self.client, path, roles, cb);
    //        },
    //        function sign(_, cb) {
    //            helper.signUrl({
    //                path: path,
    //                client: self.userClient,
    //                role: [ 'muskietest_role_limit' ]
    //            }, function (err, s) {
    //                if (err) {
    //                    cb(err);
    //                    return;
    //                }
    //                signed = s;
    //                cb();
    //            });
    //        },
    //        function get(_, cb) {
    //            self.jsonClient.get({
    //                path: signed
    //            }, function (err, req, res, obj) {
    //                if (err) {
    //                    t.fail(err);
    //                    cb(err);
    //                    return;
    //                }
    //                t.ok(obj);
    //                cb();
    //            });
    //        }
    //    ]}, function (err, results) {
    //        if (err) {
    //            t.fail(results.operations[results.ndone - 1]);
    //            t.end();
    //            return;
    //        }
    //
    //        t.end();
    //    });
    //});
    //
    //
    //suite.test('signed URL with included wrong role', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    //    var roles = 'muskietest_role_default';
    //    var signed;
    //
    //    vasync.pipeline({funcs: [
    //        function write(_, cb) {
    //            writeObject(self.client, path, roles, cb);
    //        },
    //        function sign(_, cb) {
    //            helper.signUrl({
    //                path: path,
    //                client: self.userClient,
    //                role: [ 'muskietest_role_limit' ]
    //            }, function (err, s) {
    //                if (err) {
    //                    cb(err);
    //                    return;
    //                }
    //                signed = s;
    //                cb();
    //            });
    //        },
    //        function get(_, cb) {
    //            self.jsonClient.get({
    //                path: signed
    //            }, function (err) {
    //                if (!err) {
    //                    t.fail('expected error');
    //                    cb();
    //                    return;
    //                }
    //
    //                t.equal(err.name, 'NoMatchingRoleTagError');
    //                cb();
    //            });
    //        }
    //    ]}, function (err, results) {
    //        if (err) {
    //            t.fail(results.operations[results.ndone - 1]);
    //            t.end();
    //            return;
    //        }
    //
    //        t.end();
    //    });
    //});
    //
    //
    //suite.test('signed URL with included invalid role', function (t) {
    //    var self = this;
    //    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    //    var roles = 'muskietest_role_default';
    //    var signed;
    //
    //    vasync.pipeline({funcs: [
    //        function write(_, cb) {
    //            writeObject(self.client, path, roles, cb);
    //        },
    //        function sign(_, cb) {
    //            helper.signUrl({
    //                path: path,
    //                client: self.userClient,
    //                role: [ 'muskietest_role_asdfasdf' ]
    //            }, function (err, s) {
    //                if (err) {
    //                    cb(err);
    //                    return;
    //                }
    //                signed = s;
    //                cb();
    //            });
    //        },
    //        function get(_, cb) {
    //            self.jsonClient.get({
    //                path: signed
    //            }, function (err) {
    //                if (!err) {
    //                    t.fail('expected error');
    //                    cb();
    //                    return;
    //                }
    //
    //                t.equal(err.name, 'InvalidRoleError');
    //                cb();
    //            });
    //        }
    //    ]}, function (err, results) {
    //        if (err) {
    //            t.fail(results.operations[results.ndone - 1]);
    //            t.end();
    //            return;
    //        }
    //
    //        t.end();
    //    });
    //});


    // TODO assets OK

    // TODO conditions - overwrite

    // TODO conditions - day/date/time

    // TODO conditions - sourceip

    // TODO conditions - user-agent



    suite.test('teardown', function (t) {
        client.rmr(testDir, function onRm(err) {
            t.ifError(err, 'remove test dir ' + testDir);
            t.end();
        });
    });

    suite.end();
});
