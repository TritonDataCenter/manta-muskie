/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var _helper = __dirname + '/helper.js';
if (require.cache[_helper])
    delete require.cache[_helper];
var helper = require(_helper);
var fs = require('fs');
var vasync = require('vasync');

var sdcClient;

var USERS = [
    {
        login: 'muskie_test_user',
        password: 'secret123',
        email: 'nobody+muskietest@joyent.com'
    },
    {
        login: 'muskie_test_huge_token_user',
        password: 'secret123',
        email: 'nobody+muskietest2@joyent.com'
    }
];

var POLICIES = [
    {
        name: 'muskie_test_read',
        rules: [ 'Can getobject' ]
    },
    {
        name: 'muskie_test_write',
        rules: [
            'Can putobject',
            'Can putdirectory'
        ]
    },
    {
        name: 'muskie_test_job',
        rules: [
            'Can createjob and managejob'
        ]
    },
    {
        name: 'muskie_test_create_only',
        rules: [
            'Can createjob'
        ]
    },
    {
        name: 'muskie_test_fromjob',
        rules: [
            'Can putobject and putdirectory when fromjob = false'
        ]
    },
    {
        name: 'muskie_test_mlogin',
        rules: [
            'Can mlogin'
        ]
    }
];

var ROLES = [
    {
        name: 'muskie_test_role_default',
        members: [ 'muskie_test_user' ],
        default_members: [ 'muskie_test_user' ],
        policies: [ 'muskie_test_read' ]
    },
    {
        name: 'muskie_test_role_limit',
        members: [ 'muskie_test_user' ],
        policies: [ 'muskie_test_read' ]
    },
    {
        name: 'muskie_test_role_other',
        policies: ['muskie_test_read']
    },
    {
        name: 'muskie_test_role_write',
        members: [ 'muskie_test_user' ],
        policies: [ 'muskie_test_write' ]
    },
    {
        name: 'muskie_test_role_jobs',
        members: [ 'muskie_test_user' ],
        policies: [ 'muskie_test_job', 'muskie_test_write' ]
    },
    {
        name: 'muskie_test_role_create_job',
        members: [ 'muskie_test_user' ],
        policies: [ 'muskie_test_create_only', 'muskie_test_write' ]
    },
    {
        name: 'muskie_test_role_jobs_only',
        members: [ 'muskie_test_user' ],
        policies: [ 'muskie_test_job' ]
    },
    {
        name: 'muskie_test_role_fromjob',
        members: [ 'muskie_test_user' ],
        policies: [ 'muskie_test_fromjob' ]
    },
    {
        name: 'muskie_test_role_all',
        members: [ 'muskie_test_user' ],
        policies: [ 'muskie_test_read',
                    'muskie_test_write',
                    'muskie_test_job',
                    'muskie_test_mlogin' ]
    }
];

function setup(cb) {
    var key = fs.readFileSync(process.env.HOME + '/.ssh/id_rsa.pub', 'utf8');
    vasync.pipeline({funcs: [
        function createUsers(_, pipelinecb) {
            vasync.forEachPipeline({
                func: function (user, usercb) {
                    sdcClient.createUser(user, function (err) {
                        if (err) {
                            usercb(err);
                            return;
                        }
                        sdcClient.uploadUserKey(user.login, {
                            name: 'muskie_test_key1',
                            key: key
                        }, usercb);
                    });
                },
                inputs: USERS
            }, pipelinecb);
        },
        function createPolicies(_, pipelinecb) {
            vasync.forEachPipeline({
                func: sdcClient.createPolicy.bind(sdcClient),
                inputs: POLICIES
            }, pipelinecb);
        },
        function createRoles(_, pipelinecb) {
            vasync.forEachPipeline({
                func: sdcClient.createRole.bind(sdcClient),
                inputs: ROLES
            }, pipelinecb);
        }
        /*
         * function createHugeTokenUser(_, pipelinecb) {
         *     var roleTemplate = {
         *         name: 'muskie_test_role_huge_token',
         *         members: [ 'muskie_test_huge_token_user' ],
         *         default_members: [ 'muskie_test_huge_token_user' ],
         *         policies: POLICIES.map(function (p) {
         *             return (p.name);
         *         })
         *     };
         *     var inputs = [];
         *     for (var i = 0; i < 100; ++i) {
         *         inputs[i] = i;
         *     }
         *     vasync.forEachParallel({
         *         func: function (c, parallelcb) {
         *             roleTemplate.name = 'muskie_test_role_huge_token' + c;
         *             sdcClient.createRole(roleTemplate, parallelcb);
         *         },
         *         inputs: inputs
         *     }, pipelinecb);
         * }
         */
    ]}, cb);
}


function teardown(cb) {
    vasync.pipeline({funcs: [
        function deleteRoles(_, pipelinecb) {
            sdcClient.listRoles(function (err, roles) {
                if (err) {
                    pipelinecb(err);
                    return;
                }

                var names = ROLES.map(function (r) {
                    return (r.name);
                });

                var deletions = roles.filter(function (r) {
                    return (names.indexOf(r.name) >= 0);
                });

                vasync.forEachPipeline({
                    func: sdcClient.deleteRole.bind(sdcClient),
                    inputs: deletions
                }, pipelinecb);
            });
        },
        function deletePolicies(_, pipelinecb) {
            sdcClient.listPolicies(function (err, policies) {
                if (err) {
                    pipelinecb(err);
                    return;
                }

                var names = POLICIES.map(function (p) {
                    return (p.name);
                });

                var deletions = policies.filter(function (p) {
                    return (names.indexOf(p.name) >= 0);
                });

                vasync.forEachPipeline({
                    func: sdcClient.deletePolicy.bind(sdcClient),
                    inputs: deletions
                }, pipelinecb);
            });
        },
        function deleteUsers(_, pipelinecb) {
            sdcClient.listUsers(function (err, users) {
                if (err) {
                    pipelinecb(err);
                    return;
                }

                var logins = USERS.map(function (u) {
                    return (u.login);
                });

                var deletions = users.filter(function (u) {
                    return (logins.indexOf(u.login) >= 0);
                });

                vasync.forEachPipeline({
                    func: function deleteUser(user, usercb) {
                        sdcClient.listUserKeys(user.id, function (err2, keys) {
                            if (err2) {
                                usercb(err2);
                                return;
                            }
                            vasync.forEachPipeline({
                                func: sdcClient.deleteUserKey.bind(sdcClient,
                                                                   user),
                                inputs: keys
                            }, function (err3) {
                                if (err3) {
                                    usercb(err3);
                                    return;
                                }
                                sdcClient.deleteUser(user, usercb);
                            });

                        });
                    },
                    inputs: deletions
                }, pipelinecb);
            });
        }
    ]}, cb);
}

function main() {
    if (process.argv[2] === 'setup') {
        sdcClient = helper.createSDCClient();
        setup(function (err) {
            if (err) {
                console.log(err);
            }
            sdcClient.client.close();
        });
    } else if (process.argv[2] === 'teardown') {
        sdcClient = helper.createSDCClient();
        teardown(function (err) {
            if (err) {
                console.log(err);
            }
            sdcClient.client.close();
        });
    } else {
        console.error('usage: ' + process.argv[1] + ' [setup|teardown]');
        process.exit(1);
    }
}

main();
