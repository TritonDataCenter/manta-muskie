/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var _helper = __dirname + '/helper.js';
if (require.cache[_helper])
    delete require.cache[_helper];
var helper = require(_helper);

var MemoryStream = require('stream').PassThrough;
var once = require('once');
var util = require('util');
var sprintf = util.format;
var vasync = require('vasync');

var after = helper.after;
var before = helper.before;
var test = helper.test;

///--- Helpers

function writeObject(client, key, roles, cb) {
    cb = once(cb);
    var headers;
    if (typeof (roles) === 'function') {
        cb = roles;
    } else if (typeof (roles) === 'object') {
        headers = roles;
    } else {
        headers = {
            'role-tag': roles
        };
    }
    var input = new MemoryStream();
    var msg = JSON.stringify({hello: 'world'});
    var opts = {
        type: 'application/json',
        headers: headers,
        size: Buffer.byteLength(msg)
    };
    var output = client.createWriteStream(key, opts);
    output.once('close', cb.bind(null, null));
    output.once('error', cb);
    input.pipe(output);
    input.end(msg);
}


function addTag(client, key, tag, cb) {
    client.info(key, function (err, info) {
        if (err) {
            cb(err);
            return;
        }
        var tags = info.headers['role-tag'];
        if (tags) {
            /* JSSTYLED */
            tags = tags.split(/\s*,\s*/);
        } else {
            tags = [];
        }
        var index = tags.indexOf(tag);
        if (index < 0) {
            tags.push(tag);
        }
        client.chattr(key, {
            headers: {
                'role-tag': tags.join(',')
            }
        }, cb);
    });
}


function delTag(client, key, tag, cb) {
    client.info(key, function (err, info) {
        if (err) {
            cb(err);
            return;
        }
        var tags = info.headers['role-tag'] || '';
        if (tags) {
            /* JSSTYLED */
            tags = tags.split(/\s*,\s*/);
        } else {
            tags = [];
        }
        var index = tags.indexOf(tag);
        if (index >= 0) {
            tags.splice(index, 1);
        }
        client.chattr(key, {
            headers: {
                'role-tag': tags.join(',')
            }
        }, cb);
    });
}


function jobWait(client, jobId, cb) {
    client.job(jobId, function (err, res) {
        if (err) {
            cb(err);
            return;
        }
        if (res.state !== 'done') {
            setTimeout(jobWait.bind(null, client, jobId, cb), 2000);
            return;
        }
        client.jobErrors(jobId, function (err2, errors) {
            if (err2) {
                cb(err2);
                return;
            }
            var result = [];
            errors.on('err', function (e) {
                result.push(e);
            });
            errors.once('end', function () {
                cb(null, result);
            });
        });
    });
}



///--- Tests

before(function (cb) {
    var self = this;

    self.sdcClient = helper.createSDCClient();
    self.client = helper.createClient();
    self.jsonClient = helper.createJsonClient();
    self.rawClient = helper.createRawClient();
    self.userClient = helper.createUserClient('muskie_test_user');
    self.operClient = helper.createOperatorClient();
    self.paths = [];
    self.operPaths = [];
    self.jobs = [];
    cb();
});

after(function (cb) {
    var self = this;

    vasync.forEachParallel({
        func: self.client.unlink.bind(self.client),
        inputs: self.paths
    }, function (err) {
        vasync.forEachParallel({
            func: self.client.cancelJob.bind(self.client),
            inputs: self.jobs
        }, function (err2) {
            vasync.forEachParallel({
                func: self.operClient.unlink.bind(self.operClient),
                inputs: self.operPaths
            }, function (err3) {
                self.client.close();
                self.rawClient.close();
                self.userClient.close();
                self.sdcClient.client.close();
                self.operClient.close();
                cb(err || err2 || err3);
            });
        });
    });
});



test('default role', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var roles = 'muskie_test_role_default';
    writeObject(self.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        self.paths.push(path);
        self.userClient.get(path, function (err2, res) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            t.ok(res);
            t.end();
        });
    });
});


test('inactive role', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var roles = 'muskie_test_role_limit';
    writeObject(self.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        self.paths.push(path);
        self.userClient.get(path, function (err2) {
            if (!err2) {
                t.fail(err2, 'error expected');
                t.end();
                return;
            }
            t.equal(err2.name, 'NoMatchingRoleTagError');
            t.end();
        });
    });
});


test('assume non-default role', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var roles = 'muskie_test_role_limit';
    writeObject(self.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        self.paths.push(path);
        self.userClient.get(path, {
            headers: {
                'role': 'muskie_test_role_limit'
            }
        }, function (err2, res) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            t.ok(res);
            t.end();
        });
    });
});


test('assume multiple roles', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var roles = 'muskie_test_role_limit';
    writeObject(self.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        self.paths.push(path);
        self.userClient.get(path, {
            headers: {
                'role': 'muskie_test_role_default,muskie_test_role_limit'
            }
        }, function (err2, res) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            t.ok(res);
            t.end();
        });
    });
});


test('assume wrong role', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var roles = 'muskie_test_role_default';
    writeObject(self.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        self.paths.push(path);
        self.userClient.get(path, {
            headers: {
                'role': 'muskie_test_role_limit'
            }
        }, function (err2) {
            if (!err2) {
                t.fail(err2, 'error expected');
                t.end();
                return;
            }
            t.equal(err2.name, 'NoMatchingRoleTagError');
            t.end();
        });
    });
});


test('assume limit roles (*)', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var roles = 'muskie_test_role_limit';
    writeObject(self.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        self.paths.push(path);
        self.userClient.get(path, {
            headers: {
                'role': '*'
            }
        }, function (err2, res) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            t.ok(res);
            t.end();
        });
    });
});


test('assume bad role', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var roles = 'muskie_test_role_other';
    writeObject(self.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        self.paths.push(path);
        self.userClient.get(path, {
            headers: {
                'role': 'muskie_test_role_other'
            }
        }, function (err2) {
            if (!err2) {
                t.fail(err2, 'error expected');
                t.end();
                return;
            }
            t.equal(err2.name, 'InvalidRoleError');
            t.end();
        });
    });
});


test('assume bad role - xacct', function (t) {
    var self = this;
    var path = sprintf('/%s/stor', self.operClient.user);
    self.client.get(path, {
        headers: {
            'role': 'muskie_test_role_other'
        }
    }, function (err2) {
        if (!err2) {
            t.fail(err2, 'error expected');
            t.end();
            return;
        }
        t.equal(err2.name, 'InvalidRoleError');
        t.end();
    });
});

test('mchmod', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var roles = 'muskie_test_role_write';
    writeObject(self.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }

        self.paths.push(path);
        self.userClient.chattr(path, {
            headers: {
                'role': 'muskie_test_role_write',
                'role-tag': 'muskie_test_role_other'
            }
        }, function (err2) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            self.client.info(path, function (err3, info) {
                if (err3) {
                    t.fail(err3);
                    t.end();
                    return;
                }
                t.equal(info.headers['role-tag'], 'muskie_test_role_other');
                t.end();
            });
        });
    });
});

/*
 * Tests for scenarios around rules with the "*"" or "all" aperture
 * resource (support added with MANTA-3962).
 */
test('all-resource rules (untagged)', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var role = 'muskie_test_role_star';
    /* First, create a test object, with no role tags. */
    writeObject(self.client, path, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        self.paths.push(path);

        /*
         * This should not work: we haven't activated the role and we have
         * no default roles that are tagged on the object, so we have no
         * right to read it.
         */
        self.userClient.info(path, function (err2) {
            if (!err2) {
                t.fail('error expected');
                t.end();
                return;
            }

            /*
             * This should not work either: the role has a rule "Can putobject"
             * but without the * this doesn't apply to all objects, only role-
             * tagged ones, and this object has no role-tag.
             */
            writeObject(self.userClient, path, {
                'role': role
            }, function (err3) {
                if (!err3) {
                    t.fail('error expected');
                    t.end();
                    return;
                }

                /*
                 * This should work, though: the "Can getobject *" rule kicks
                 * in, even though this object isn't tagged (thanks to the *).
                 */
                self.userClient.info(path, {
                    headers: {
                        'role': role
                    }
                }, function (err4, info) {
                    if (err4) {
                        t.fail(err4);
                        t.end();
                        return;
                    }
                    t.strictEqual(info.headers['role-tag'], undefined);
                    t.end();
                });
            });
        });
    });
});

test('all-resource rules (tagged)', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var role = 'muskie_test_role_star';
    /* First, create a test object, this time tagged to the role. */
    writeObject(self.client, path, role, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        self.paths.push(path);

        /*
         * We should be able to write it, since it's role-tagged so the
         * "Can putobject" rule applies.
         */
        writeObject(self.userClient, path, {
            'role': role,
            'role-tag': role
        }, function (err2) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            /*
             * And we should also be able to read it thanks to the
             * "Can getobject *" rule.
             */
            self.userClient.info(path, {
                headers: {
                    'role': role
                }
            }, function (err3, info) {
                if (err3) {
                    t.fail(err3);
                    t.end();
                    return;
                }
                t.equal(info.headers['role-tag'], role);
                t.end();
            });
        });
    });
});

/*
 * Tests for scenarios around rules with explicit resource strings (support
 * added with MANTA-4284).
 */
test('explicit resource rules', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_glob_1', self.client.user);
    var role = 'muskie_test_role_glob';
    /* First, create a test object, with no role tags. */
    writeObject(self.client, path, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        self.paths.push(path);

        /*
         * This should not work: we haven't activated the role and we have
         * no default roles that are tagged on the object, so we have no
         * right to read it.
         */
        self.userClient.info(path, function (err2) {
            if (!err2) {
                t.fail('error expected');
                t.end();
                return;
            }

            /*
             * This should work, though: the "Can getobject /..." rule kicks
             * in, even though this object isn't tagged.
             */
            self.userClient.info(path, {
                headers: {
                    'role': role
                }
            }, function (err4, info) {
                if (err4) {
                    t.fail(err4);
                    t.end();
                    return;
                }
                t.strictEqual(info.headers['role-tag'], undefined);
                t.end();
            });
        });
    });
});

test('explicit resource rules (denied)', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_noglob', self.client.user);
    var role = 'muskie_test_role_glob';
    /* First, create a test object, with no role tags. */
    writeObject(self.client, path, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        self.paths.push(path);

        /*
         * This should not work: we haven't activated the role and we have
         * no default roles that are tagged on the object, so we have no
         * right to read it.
         */
        self.userClient.info(path, function (err2) {
            if (!err2) {
                t.fail('error expected');
                t.end();
                return;
            }

            /*
             * This should not work, either: the rule with the explicit
             * resource on muskie_test_role_glob does not match the path.
             */
            self.userClient.info(path, {
                headers: {
                    'role': role
                }
            }, function (err4, info) {
                if (!err4) {
                    t.fail('error expected');
                }
                t.end();
            });
        });
    });
});

test('cross-account role access (denied)', function (t) {
    var self = this;
    var path = sprintf('/%s/stor', self.operClient.user);
    self.client.info(path, {
        headers: {
            'role': 'muskie_test_role_xacct'
        }
    }, function (err3, info) {
        if (!err3) {
            t.fail('error expected');
            t.end();
            return;
        }
        t.equal(err3.name, 'ForbiddenError');

        self.client.info(path, function (err4, info2) {
            if (!err4) {
                t.fail('error expected');
                t.end();
                return;
            }
            t.equal(err4.name, 'ForbiddenError');
            t.end();
        });
    });
});

test('cross-account role access', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.operClient.user);
    writeObject(self.operClient, path, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }

        self.operPaths.push(path);
        self.operClient.chattr(path, {
            headers: {
                'role-tag': 'muskie_test_role_xacct'
            }
        }, function (err2) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            self.client.info(path, {
                headers: {
                    'role': 'muskie_test_role_xacct'
                }
            }, function (err3, info) {
                if (err3) {
                    t.fail(err3);
                    t.end();
                    return;
                }
                t.equal(info.headers['role-tag'], 'muskie_test_role_xacct');
                t.end();
            });
        });
    });
});

test('mchmod bad role', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var roles = 'muskie_test_role_write';
    writeObject(self.client, path, roles, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }

        self.paths.push(path);
        self.userClient.chattr(path, {
            headers: {
                'role': 'muskie_test_role_write',
                'role-tag': 'asdf'
            }
        }, function (err2) {
            if (!err2) {
                t.fail('error expected');
                t.end();
                return;
            }
            t.equal(err2.name, 'InvalidRoleTagError');
            t.end();
        });
    });
});


test('created object gets roles', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var dir = sprintf('/%s/stor', self.client.user);
    addTag(self.client, dir, 'muskie_test_role_write', function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        writeObject(self.userClient, path, {
            'role': 'muskie_test_role_write'
        }, function (err2) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            self.paths.push(path);

            self.client.info(path, function (err3, info) {
                if (err3) {
                    t.fail(err3);
                    t.end();
                    return;
                }

                /* JSSTYLED */
                var tags = info.headers['role-tag'].split(/\s*,\s*/);
                t.ok(tags.indexOf('muskie_test_role_write') >= 0);

                delTag(self.client, dir, 'muskie_test_role_write',
                        function (err4) {

                    if (err4) {
                        t.fail(err4);
                        t.end();
                        return;
                    }
                    t.end();
                });
            });
        });
    });
});


test('create object parent directory check', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    writeObject(self.userClient, path, {
        'role': 'muskie_test_role_write'
    }, function (err2) {
        if (!err2) {
            self.paths.push(path);
            t.fail('expected error');
            t.end();
            return;
        }

        t.equal(err2.name, 'NoMatchingRoleTagError');
        t.end();
    });
});


test('create object parent directory check', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    writeObject(self.userClient, path, {
        'role': 'muskie_test_role_write'
    }, function (err) {
        if (!err) {
            self.paths.push(path);
            t.fail('expected error');
            t.end();
            return;
        }

        t.equal(err.name, 'NoMatchingRoleTagError');
        t.end();
    });
});


test('create directory parent directory check', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_dir', self.client.user);
    self.userClient.mkdir(path, {
        headers: {
            'role': 'muskie_test_role_write'
        }
    }, function (err2) {
        if (!err2) {
            self.paths.push(path);
            t.fail('expected error');
            t.end();
            return;
        }

        t.equal(err2.name, 'NoMatchingRoleTagError');
        t.end();
    });
});


// Ideally, getting a nonexistent object should mean a check on the parent
// directory to see if the user has read permissions on the directory. However,
// since this requires an additional lookup, we're just returning 404s for now.
test('get nonexistent object 404', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_dir', self.client.user);
    self.client.get(path, function (err2) {
        if (!err2) {
            t.fail('error expected');
            t.end();
            return;
        }
        t.equal(err2.name, 'ResourceNotFoundError');
        t.end();
    });
});


test('signed URL uses default roles', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var roles = 'muskie_test_role_default';
    var signed;

    vasync.pipeline({funcs: [
        function write(_, cb) {
            writeObject(self.client, path, roles, cb);
        },
        function sign(_, cb) {
            helper.signUrl({
                path: path,
                client: self.userClient
            }, function (err, s) {
                if (err) {
                    cb(err);
                    return;
                }
                signed = s;
                cb();
            });
        },
        function get(_, cb) {
            self.jsonClient.get({
                path: signed
            }, function (err, req, res, obj) {
                if (err) {
                    t.fail(err);
                    cb(err);
                    return;
                }
                t.ok(obj);
                cb();
            });
        }
    ]}, function (err, results) {
        if (err) {
            t.fail(results.operations[results.ndone - 1]);
            t.end();
            return;
        }

        t.end();
    });
});


test('signed URL ignores role headers', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var roles = 'muskie_test_role_limit';
    var signed;

    vasync.pipeline({funcs: [
        function write(_, cb) {
            writeObject(self.client, path, roles, cb);
        },
        function sign(_, cb) {
            helper.signUrl({
                path: path,
                client: self.userClient
            }, function (err, s) {
                if (err) {
                    cb(err);
                    return;
                }
                signed = s;
                cb();
            });
        },
        function get(_, cb) {
            self.jsonClient.get({
                path: signed,
                headers: {
                    role: 'muskie_test_role_limit'
                }
            }, function (err) {
                if (!err) {
                    t.fail('expected error');
                    cb();
                    return;
                }

                t.equal(err.name, 'NoMatchingRoleTagError');
                cb();
            });
        }
    ]}, function (err, results) {
        if (err) {
            t.fail(results.operations[results.ndone - 1]);
            t.end();
            return;
        }

        t.end();
    });
});


test('signed URL with included role', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var roles = 'muskie_test_role_limit';
    var signed;

    vasync.pipeline({funcs: [
        function write(_, cb) {
            writeObject(self.client, path, roles, cb);
        },
        function sign(_, cb) {
            helper.signUrl({
                path: path,
                client: self.userClient,
                role: [ 'muskie_test_role_limit' ]
            }, function (err, s) {
                if (err) {
                    cb(err);
                    return;
                }
                signed = s;
                cb();
            });
        },
        function get(_, cb) {
            self.jsonClient.get({
                path: signed
            }, function (err, req, res, obj) {
                if (err) {
                    t.fail(err);
                    cb(err);
                    return;
                }
                t.ok(obj);
                cb();
            });
        }
    ]}, function (err, results) {
        if (err) {
            t.fail(results.operations[results.ndone - 1]);
            t.end();
            return;
        }

        t.end();
    });
});


test('signed URL with included wrong role', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var roles = 'muskie_test_role_default';
    var signed;

    vasync.pipeline({funcs: [
        function write(_, cb) {
            writeObject(self.client, path, roles, cb);
        },
        function sign(_, cb) {
            helper.signUrl({
                path: path,
                client: self.userClient,
                role: [ 'muskie_test_role_limit' ]
            }, function (err, s) {
                if (err) {
                    cb(err);
                    return;
                }
                signed = s;
                cb();
            });
        },
        function get(_, cb) {
            self.jsonClient.get({
                path: signed
            }, function (err) {
                if (!err) {
                    t.fail('expected error');
                    cb();
                    return;
                }

                t.equal(err.name, 'NoMatchingRoleTagError');
                cb();
            });
        }
    ]}, function (err, results) {
        if (err) {
            t.fail(results.operations[results.ndone - 1]);
            t.end();
            return;
        }

        t.end();
    });
});


test('signed URL with included invalid role', function (t) {
    var self = this;
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var roles = 'muskie_test_role_default';
    var signed;

    vasync.pipeline({funcs: [
        function write(_, cb) {
            writeObject(self.client, path, roles, cb);
        },
        function sign(_, cb) {
            helper.signUrl({
                path: path,
                client: self.userClient,
                role: [ 'muskie_test_role_asdfasdf' ]
            }, function (err, s) {
                if (err) {
                    cb(err);
                    return;
                }
                signed = s;
                cb();
            });
        },
        function get(_, cb) {
            self.jsonClient.get({
                path: signed
            }, function (err) {
                if (!err) {
                    t.fail('expected error');
                    cb();
                    return;
                }

                t.equal(err.name, 'InvalidRoleError');
                cb();
            });
        }
    ]}, function (err, results) {
        if (err) {
            t.fail(results.operations[results.ndone - 1]);
            t.end();
            return;
        }

        t.end();
    });
});


test('create job ACL check failure', function (t) {
    var self = this;
    self.userClient.createJob({
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    }, function (err, jobId) {
        if (jobId) {
            self.jobs.push(jobId);
        }

        if (!err) {
            t.fail('error expected');
            t.end();
            return;
        }

        t.equal(err.name, 'NoMatchingRoleTagError');
        t.end();
    });
});


test('create job ACL check success', function (t) {
    var self = this;
    var path = sprintf('/%s/jobs', self.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    };
    addTag(self.client, path, 'muskie_test_role_jobs', function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }

        self.userClient.createJob(job, {
            headers: {
                role: 'muskie_test_role_jobs'
            }
        }, function (err2, jobId) {
            if (jobId) {
                self.jobs.push(jobId);
            }

            delTag(self.client, path, 'muskie_test_role_jobs',
                    function (err3) {

                if (err2 || err3) {
                    t.fail(err2 || err3);
                    t.end();
                    return;
                }

                t.end();
            });
        });
    });
});


test('job inputs - no managejob on /jobs', function (t) {
    var self = this;
    var jobRoot = sprintf('/%s/jobs', self.client.user);
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    };
    writeObject(self.client, path, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        self.paths.push(path);

        addTag(self.client, jobRoot, 'muskie_test_role_create_job',
                function (err2) {

            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            self.userClient.createJob(job, {
                headers: {
                    role: 'muskie_test_role_create_job'
                }
            }, function (err3, jobId) {
                if (jobId) {
                    self.jobs.push(jobId);
                }
                if (err3) {
                    t.fail(err3);
                    t.end();
                    return;
                }

                self.userClient.addJobKey(jobId, path, function (err4) {
                    if (!err4) {
                        t.fail('expected error');
                    } else {
                        t.equal(err4.name, 'NoMatchingRoleTagError');
                    }

                    delTag(self.client, path, 'muskie_test_role_create_job',
                            function (err5) {

                        if (err5) {
                            t.fail(err5);
                            t.end();
                            return;
                        }

                        t.end();
                    });
                });
            });
        });
    });
});


test('job inputs - no managejob active', function (t) {
    var self = this;
    var jobRoot = sprintf('/%s/jobs', self.client.user);
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    };
    writeObject(self.client, path, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        self.paths.push(path);

        addTag(self.client, jobRoot, 'muskie_test_role_jobs', function (err2) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            self.userClient.createJob(job, {
                headers: {
                    role: 'muskie_test_role_jobs'
                }
            }, function (err3, jobId) {
                if (jobId) {
                    self.jobs.push(jobId);
                }
                if (err3) {
                    t.fail(err3);
                    t.end();
                    return;
                }

                self.userClient.addJobKey(jobId, path, function (err4) {
                    if (!err4) {
                        t.fail('expected error');
                    } else {
                        t.equal(err4.name, 'NoMatchingRoleTagError');
                    }

                    delTag(self.client, path, 'muskie_test_role_jobs',
                            function (err5) {

                        if (err5) {
                            t.fail(err5);
                            t.end();
                            return;
                        }

                        t.end();
                    });
                });
            });
        });
    });
});


test('job inputs - no getobject on input key', function (t) {
    var self = this;
    var jobRoot = sprintf('/%s/jobs', self.client.user);
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    };

    writeObject(self.client, path, function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        self.paths.push(path);

        addTag(self.client, jobRoot, 'muskie_test_role_jobs', function (err2) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            self.userClient.createJob(job, {
                headers: {
                    role: 'muskie_test_role_jobs'
                }
            }, function (err3, jobId) {
                if (jobId) {
                    self.jobs.push(jobId);
                }
                if (err3) {
                    t.fail(err3);
                    t.end();
                    return;
                }

                self.userClient.addJobKey(jobId, path, {
                    headers: {
                        role: 'muskie_test_role_jobs'
                    }
                }, function (err4) {
                    delTag(self.client, jobRoot, 'muskie_test_role_jobs',
                            function (err5) {

                        if (err4 || err5) {
                            t.fail(err4 || err5);
                            t.end();
                            return;
                        }

                        function checkJob() {
                            self.client.job(jobId, function (err7, res) {
                                if (err7) {
                                    t.fail(err7);
                                    t.end();
                                    return;
                                }

                                if (res.state !== 'done') {
                                    setTimeout(checkJob, 2000);
                                    return;
                                }

                                self.client.jobErrors(jobId,
                                        function (err8, errors) {

                                    if (err8) {
                                        t.fail(err8);
                                        t.end();
                                        return;
                                    }

                                    var list = [];

                                    errors.on('err', function (e) {
                                        list.push(e);
                                    });

                                    errors.once('end', function () {
                                        t.equal(list.length, 1);
                                        if (list.length !== 1) {
                                            t.end();
                                            return;
                                        }
                                        t.equal(list[0].code,
                                            'AuthorizationError');
                                        t.end();
                                    });
                                });
                            });
                        }

                        self.client.endJob(jobId, function (err6) {
                            if (err6) {
                                t.fail(err6);
                                t.end();
                                return;
                            }
                            self.jobs.pop();
                            setTimeout(checkJob, 2000);
                        });
                    });
                });
            });
        });
    });
});


test('job inputs - context change after job creation', function (t) {
    var self = this;
    var jobRoot = sprintf('/%s/jobs', self.client.user);
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    };
    var jobId;

    vasync.pipeline({funcs: [
        function write(_, cb) {
            writeObject(self.client, path, 'muskie_test_role_default',
                    function (err) {

                if (err) {
                    cb(err);
                    return;
                }
                self.paths.push(path);
                cb();
            });
        },
        function tag(_, cb) {
            addTag(self.client, jobRoot, 'muskie_test_role_jobs_only', cb);
        },
        function create(_, cb) {
            self.userClient.createJob(job, {
                headers: {
                    role: 'muskie_test_role_jobs_only, ' +
                          'muskie_test_role_fromjob, ' +
                          'muskie_test_role_default'
                }
            }, function (err, id) {
                if (err) {
                    cb(err);
                    return;
                }

                jobId = id;
                cb();
            });
        },
        function input(_, cb) {
            self.userClient.addJobKey(jobId, path, {
                headers: {
                    role: 'muskie_test_role_jobs_only'
                }
            }, cb);
        },
        function end(_, cb) {
            self.client.endJob(jobId, cb);
        },
        function check(_, cb) {
            jobWait(self.client, jobId, function (err, errors) {
                if (err) {
                    cb(err);
                    return;
                }
                if (errors.length < 1) {
                    t.fail('error expected');
                    cb();
                    return;
                }
                t.equal(errors[0].code, 'InternalError');
                cb();
            });
        }
    ]}, function (err, results) {
        delTag(self.client, jobRoot, 'muskie_test_role_jobs_only',
                function (err2) {

            if (err) {
                t.fail(results.operations[results.ndone - 1]);
                t.end();
                return;
            }

            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            t.end();
        });
    });
});


test('no putdirectory on job creation', function (t) {
    var self = this;
    var jobRoot = sprintf('/%s/jobs', self.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    };

    addTag(self.client, jobRoot, 'muskie_test_role_jobs_only', function (err2) {
        if (err2) {
            t.fail(err2);
            t.end();
            return;
        }

        self.userClient.createJob(job, {
            headers: {
                role: 'muskie_test_role_jobs_only'
            }
        }, function (err3, jobId) {
            if (jobId) {
                self.jobs.push(jobId);
            }

            delTag(self.client, jobRoot, 'muskie_test_role_jobs_only',
                    function (err4) {

                if (!err3) {
                    t.fail('expected error');
                    t.end();
                    return;
                }

                if (err4) {
                    t.fail(err4);
                    t.end();
                    return;
                }

                t.equal(err3.restCode, 'MissingPermission');
                t.end();
            });
        });
    });
});


test('job OK', function (t) {
    var self = this;
    var jobRoot = sprintf('/%s/jobs', self.client.user);
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            exec: 'wc'
        } ]
    };

    writeObject(self.client, path, 'muskie_test_role_default', function (err) {
        if (err) {
            t.fail(err);
            t.end();
            return;
        }
        self.paths.push(path);

        addTag(self.client, jobRoot, 'muskie_test_role_jobs', function (err2) {
            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            self.userClient.createJob(job, {
                headers: {
                    role: 'muskie_test_role_jobs, ' +
                            'muskie_test_role_default, ' +
                            'muskie_test_role_write'
                }
            }, function (err3, jobId) {
                if (jobId) {
                    self.jobs.push(jobId);
                }
                if (err3) {
                    t.fail(err3);
                    t.end();
                    return;
                }

                self.userClient.addJobKey(jobId, path, {
                    headers: {
                        role: 'muskie_test_role_jobs'
                    }
                }, function (err4) {
                    delTag(self.client, jobRoot, 'muskie_test_role_jobs',
                            function (err5) {

                        if (err4 || err5) {
                            t.fail(err4 || err5);
                            t.end();
                            return;
                        }

                        function checkJob() {
                            self.client.job(jobId, function (err7, res) {
                                if (err7) {
                                    t.fail(err7);
                                    t.end();
                                    return;
                                }

                                if (res.state !== 'done') {
                                    setTimeout(checkJob, 2000);
                                    return;
                                }

                                self.client.jobErrors(jobId,
                                        function (err8, errors) {

                                    if (err8) {
                                        t.fail(err8);
                                        t.end();
                                        return;
                                    }

                                    var list = [];

                                    errors.on('err', function (e) {
                                        list.push(e);
                                    });

                                    errors.once('end', function () {
                                        if (list.length > 0) {
                                            t.fail(list[0]);
                                            t.end();
                                            return;
                                        }
                                        t.equal(list.length, 0);
                                        t.end();
                                    });
                                });
                            });
                        }

                        self.client.endJob(jobId, function (err6) {
                            if (err6) {
                                t.fail(err6);
                                t.end();
                                return;
                            }
                            self.jobs.pop();
                            setTimeout(checkJob, 2000);
                        });
                    });
                });
            });
        });
    });
});


test('assets - no getobject on asset', function (t) {
    var self = this;
    var jobRoot = sprintf('/%s/jobs', self.client.user);
    var path = sprintf('/%s/stor/muskie_test_obj', self.client.user);
    var asset = sprintf('/%s/stor/muskie_test_aasset', self.client.user);
    var job = {
        name: 'muskie_test_word_count',
        phases: [ {
            type: 'map',
            assets: [ asset ],
            exec: 'wc'
        } ]
    };
    var jobId;

    vasync.pipeline({funcs: [
        function writeobj(_, cb) {
            writeObject(self.client, path, 'muskie_test_role_default',
                    function (err) {

                if (err) {
                    cb(err);
                    return;
                }
                self.paths.push(path);
                cb();
            });
        },
        function writeasset(_, cb) {
            writeObject(self.client, asset, function (err) {
                if (err) {
                    cb(err);
                    return;
                }
                self.paths.push(asset);
                cb();
            });
        },
        function tag(_, cb) {
            addTag(self.client, jobRoot, 'muskie_test_role_jobs', cb);
        },
        function create(_, cb) {
            self.userClient.createJob(job, {
                headers: {
                    role: 'muskie_test_role_jobs'
                }
            }, function (err, id) {
                if (err) {
                    cb(err);
                    return;
                }

                jobId = id;
                cb();
            });
        },
        function input(_, cb) {
            self.userClient.addJobKey(jobId, path, {
                headers: {
                    role: 'muskie_test_role_jobs'
                }
            }, cb);
        },
        function end(_, cb) {
            self.client.endJob(jobId, cb);
        },
        function check(_, cb) {
            jobWait(self.client, jobId, function (err, errors) {
                if (err) {
                    cb(err);
                    return;
                }
                if (errors.length < 1) {
                    t.fail('error expected');
                    cb();
                    return;
                }
                t.equal(errors[0].code, 'AuthorizationError');
                cb();
            });
        }
    ]}, function (err, results) {
        delTag(self.client, jobRoot, 'muskie_test_role_jobs',
                function (err2) {

            if (err) {
                t.fail(results.operations[results.ndone - 1]);
                t.end();
                return;
            }

            if (err2) {
                t.fail(err2);
                t.end();
                return;
            }

            t.end();
        });
    });
});

// TODO assets OK

// TODO conditions - overwrite

// TODO conditions - day/date/time

// TODO conditions - sourceip

// TODO conditions - user-agent
