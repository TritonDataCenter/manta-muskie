/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var crypto = require('crypto');

var MemoryStream = require('stream').PassThrough;
var uuid = require('node-uuid');
var vasync = require('vasync');

if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');



///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;

var TEXT = 'The lazy brown fox \nsomething \nsomething foo';



///--- Helpers

function getState(opts, cb) {
    opts.client.job(opts.job, function (err, job) {
        if (err) {
            cb(err);
        } else if (job.state === 'done') {
            cb();
        } else if (++opts.attempts === (opts.maxPoll || 30)) {
            cb(new Error('job didn\'t finish'));
        } else {
            setTimeout(getState.bind(this, opts, cb), 1000);
        }
    });
}


function writeObject(client, key, opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    var _opts = {
        headers: opts.headers,
        md5: crypto.createHash('md5').update(TEXT).digest('base64'),
        size: Buffer.byteLength(TEXT),
        type: 'text/plain'
    };
    var stream = new MemoryStream();

    client.put(key, stream, _opts, cb);
    process.nextTick(stream.end.bind(stream, TEXT));
}


var JOB_PIPELINE = [
    function create(opts, cb) {
        opts.client.createJob('wc', function (err, job) {
            if (err) {
                cb(err);
                return;
            }
            opts.job = job;
            cb();
        });
    },
    function addKeys(opts, cb) {
        opts.client.addJobKey(opts.job, opts.keys, {end: true}, cb);
    },
    function waitForDone(opts, cb) {
        opts.attempts = 0;
        getState(opts, cb);
    }

];


function pollUntilRunning(client, jid, cb) {
    client.job(jid, function (err, j) {
        if (err) {
            cb(err);
            return;
        }
        if (j.state === 'running') {
            cb(null, j);
            return;
        }
        setTimeout(pollUntilRunning.bind(null, client, jid, cb), 1000);
    });
}



///--- Tests

before(function (cb) {
    var self = this;

    this.client = helper.createClient();
    this.jobs_root = '/' + this.client.user + '/jobs';
    this.root = '/' + this.client.user + '/stor';
    this.dir = this.root + '/' + uuid.v4();
    this.keys = [];

    var num_keys = parseInt(process.env.JOB_KEYS || 1, 10);
    for (var i = 0; i < num_keys; i++)
        this.keys.push(this.dir +'/' + uuid.v4());

    this.checkContent = function checkContent(t, stream, res) {
        t.ok(stream);
        t.ok(res);
        t.checkResponse(res, 200);
        t.equal(res.headers['content-type'], 'text/plain');
        t.ok(res.headers.etag);
        t.ok(res.headers['last-modified']);

        stream.setEncoding('utf8');
        var body = '';
        stream.on('data', function (chunk) {
            body += chunk;
        });
        stream.once('error', function (err) {
            t.ifError(err);
            t.end();
        });
        stream.once('end', function () {
            t.equal(body, TEXT);
            t.end();
        });
    };

    this.client.mkdir(this.dir, function (mkdir_err) {
        if (mkdir_err) {
            cb(mkdir_err);
            return;
        }

        vasync.forEachParallel({
            func: writeObject.bind(null, self.client),
            inputs: self.keys
        }, function (err, res) {
            if (err) {
                cb(err);
                return;
            }

            cb(null);
        });
    });
});


after(function (cb) {
    var self = this;

    this.client.rmr(this.dir, function () {
        if (self.job) {
            self.client.endJob(self.job, cb.bind(null, null));
            return;
        }
        cb();
    });
});


test('create and get job', function (t) {
    var self = this;
    var j = {
        name: 'unit_test',
        phases: [ {
            assets: [ self.keys[0] ],
            exec: 'wc',
            type: 'reduce'
        }, {
            type: 'reduce',
            memory: 256,
            count: 1,
            exec: 'awk \'{ l += $1; w += $2; c += $3 } END ' +
                '{ print l, w, c }\''
        } ]
    };
    this.client.createJob(j, function (err, jid) {
        t.ifError(err);
        t.ok(jid);
        if (!jid) {
            t.end();
            return;
        }
        self.job = jid;
        self.client.job(jid, function (err2, job) {
            t.ifError(err2);
            t.equal(job.id, jid);
            t.equal(job.name, 'unit_test');
            t.ok(job.state);
            t.ok(!job.cancelled);
            t.ok(!job.inputDone);
            t.ok(job.timeCreated);
            t.ok(job.phases);
            t.deepEqual(job.phases, j.phases);
            t.end();
        });
    });
});


test('create and end job', function (t) {
    var self = this;
    this.client.createJob('wc', function (err, jid) {
        t.ifError(err);
        t.ok(jid);
        self.client.endJob(jid, function (err2) {
            t.ifError(err2);
            self.client.job(jid, function (err3, job) {
                t.ifError(err3);
                t.ok(job);
                t.ok(job.inputDone);
                t.end();
            });
        });
    });
});


test('get job output', function (t) {
    var self = this;
    var opts = {
        funcs: JOB_PIPELINE,
        arg: {
            client: self.client,
            keys: self.keys,
            t: t
        }
    };
    vasync.pipeline(opts, function (err) {
        t.ifError(err);
        if (err) {
            t.end();
            return;
        }

        var j = opts.arg.job;
        self.client.jobOutput(j, function (err2, res) {
            t.ifError(err2);
            if (err2) {
                t.end();
                return;
            }

            var keys = 0;
            res.on('key', function (k) {
                t.ok(k);
                keys++;
            });

            res.once('error', function (err3) {
                t.ifError(err3);
                t.end();
            });

            res.once('end', function () {
                t.equal(keys, self.keys.length);
                t.end();
            });
        });
    });
});


test('create job no phases', function (t) {
    this.client.createJob({}, function (err, jid) {
        t.ok(err);
        t.equal(err.name, 'InvalidJobError');
        t.ok(err.message);
        t.end();
    });
});


test('cancel job', function (t) {
    var self = this;
    this.client.createJob('wc', function (err, jid) {
        t.ifError(err);
        t.ok(jid);
        if (!jid) {
            t.end();
            return;
        }
        self.client.cancelJob(jid, function (err2) {
            t.ifError(err2);
            self.client.job(jid, function (err3, job) {
                t.ifError(err3);
                t.equal(job.id, jid);
                t.ok(job.cancelled);
                t.end();
            });
        });
    });
});


test('cancel job after cancel', function (t) {
    var self = this;
    this.client.createJob('wc', function (err, jid) {
        t.ifError(err);
        t.ok(jid);
        if (!jid) {
            t.end();
            return;
        }
        self.client.cancelJob(jid, function (err2) {
            t.ifError(err2);
            self.client.cancelJob(jid, function (err3) {
                t.ok(err3);
                t.equal(err3.name, 'InvalidJobStateError');
                t.end();
            });
        });
    });
});


test('list jobs', function (t) {
    this.client.listJobs(function (err, stream) {
        t.ifError(err);
        t.ok(stream);

        var jobs = 0;
        stream.on('job', function (j) {
            t.ok(j);
            jobs++;
        });

        stream.once('end', function () {
            t.ok(jobs > 0);
            t.end();
        });

    });
});


test('list jobs, bad state', function (t) {
    this.client.listJobs({ 'state': 'nonsense'}, function (err, stream) {
        t.ifError(err);
        t.ok(stream);

        stream.on('error', function (err2) {
            t.ok(err2);
            t.equal(err2.name, 'InvalidParameterError');
            t.ok(err2.message);
            t.end();
        });

        stream.on('end', function (end) {
            t.end();
        });
    });
});


test('list jobs, bad name', function (t) {
    this.client.listJobs({ 'name': 'bad@bad'}, function (err, stream) {
        t.ifError(err);
        t.ok(stream);

        stream.on('error', function (err2) {
            t.ok(err2);
            t.equal(err2.name, 'InvalidParameterError');
            t.ok(err2.message);
            t.end();
        });

        stream.on('end', function (end) {
            t.end();
        });
    });
});


test('list job by name', function (t) {
    var self = this;
    var name = 'search_by_name';
    var j = {
        name: name,
        phases: [ {
            exec: 'wc',
            type: 'map'
        } ]
    };
    this.client.createJob(j, function (err, jid) {
        t.ifError(err);
        t.ok(jid);
        if (!jid) {
            t.end();
            return;
        }
        self.job = jid;

        pollUntilRunning(self.client, jid, function (err2, jr) {
            t.ifError(err2);
            t.equal(jid, jr.id);
            t.equal(name, jr.name);

            var opts = { 'name': name };
            self.client.listJobs(opts, function (err3, s) {
                t.ifError(err);
                t.ok(s);

                var jobs = [];
                s.once('error', function (err4) {
                    t.ifError(err4);
                    t.end();
                });

                s.on('job', function (job) {
                    jobs.push(job);
                });

                s.on('end', function (end) {
                    t.ok(jobs.length > 0);
                    var found = false;
                    var sjid = self.job.id;
                    for (var i = 0; i < jobs.length; ++i) {
                        if (jobs[i].id === sjid) {
                            found = true;
                            break;
                        }
                    }
                    t.ok(found);
                    var x = jid;
                    self.client.cancelJob(x, function (e) {
                        t.ifError(e);
                        t.end();
                    });
                });
            });
        });
    });
});


test('get job 404', function (t) {
    this.client.job(uuid.v4(), function (err) {
        t.ok(err);
        t.equal(err.name, 'ResourceNotFoundError');
        t.ok(err.message);
        t.end();
    });
});


test('add job keys', function (t) {
    var self = this;
    this.client.createJob('wc', function (err, j) {
        t.ifError(err);
        t.ok(j);
        self.job = j;
        self.client.addJobKey(j, self.keys, function (err2) {
            t.ifError(err2);

            self.client.jobInput(j, function (err3, stream) {
                t.ifError(err3);
                t.ok(stream);

                var keys = 0;
                stream.on('key', function (k) {
                    t.ok(k);
                    t.ok(self.keys.some(function (i) {
                        return (i === k);
                    }));
                    keys++;
                });

                stream.once('end', function () {
                    t.equal(keys, self.keys.length);
                    t.end();
                });
            });
        });
    });
});


test('add job keys after input done', function (t) {
    var self = this;
    this.client.createJob('wc', function (err, j) {
        t.ifError(err);
        t.ok(j);

        self.client.endJob(j, function (err2) {
            t.ifError(err2);
            self.client.addJobKey(j, self.keys, function (err3) {
                t.ok(err3);
                t.equal(err3.name, 'InvalidJobStateError');
                t.end();
            });
        });
    });
});


test('add job keys after cancel', function (t) {
    var self = this;
    this.client.createJob('wc', function (err, j) {
        t.ifError(err);
        t.ok(j);

        self.client.cancelJob(j, function (err2) {
            t.ifError(err2);
            self.client.addJobKey(j, self.keys, function (err3) {
                t.ok(err3);
                t.equal(err3.name, 'InvalidJobStateError');
                t.end();
            });
        });
    });
});


test('end after cancel', function (t) {
    var self = this;
    this.client.createJob('wc', function (err, j) {
        t.ifError(err);
        t.ok(j);

        self.client.cancelJob(j, function (err2) {
            t.ifError(err2);
            self.client.endJob(j, function (err3) {
                t.ok(err3);
                t.equal(err3.name, 'InvalidJobStateError');
                t.end();
            });
        });
    });
});


test('cancel after end', function (t) {
    var self = this;
    this.client.createJob('wc', function (err, j) {
        t.ifError(err);
        t.ok(j);

        self.client.endJob(j, function (err2) {
            t.ifError(err2);
            self.client.cancelJob(j, function (err3) {
                if (err3) {
                    t.ok(err3);
                    t.equal(err3.name,
                            'InvalidJobStateError');
                }
                t.end();
            });
        });
    });
});


// This is unreliable because the job will often be 'done' before
// the second end is called.
//
// test('end after end', function (t) {
//         var self = this;
//         this.client.createJob('wc', function (err, j) {
//                 t.ifError(err);
//                 t.ok(j);

//                 self.client.endJob(j, function (err2) {
//                         t.ifError(err2);
//                         self.client.endJob(j, function (err3) {
//                                 t.ifError(err3);
//                                 t.end();
//                         });
//                 });
//         });
// });


test('get job failures', function (t) {
    var self = this;
    var opts = {
        funcs: JOB_PIPELINE,
        arg: {
            client: self.client,
            keys: ['/poseidon/stor/' + uuid.v4()],
            t: t
        }
    };
    vasync.pipeline(opts, function (err) {
        t.ifError(err);
        self.client.jobFailures(opts.arg.job, function (err2, res) {
            t.ifError(err2);
            if (err2) {
                t.end();
                return;
            }

            var keys = 0;
            res.on('key', function (k) {
                t.ok(k);
                t.ok(opts.arg.keys.some(function (i) {
                    return (i === k);
                }));
                keys++;
            });

            res.once('error', function (err3) {
                t.ifError(err3);
                t.end();
            });

            res.once('end', function () {
                t.equal(keys, self.keys.length);
                t.end();
            });
        });
    });
});
