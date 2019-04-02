/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var assert = require('assert-plus');
var testHelper = require('../helper.js');

function initBucketTester(next) {
    var self = this;

    self.client = testHelper.createClient();
    self.userClient = testHelper.createUserClient('muskie_test_user');
    self.operatorClient = testHelper.createOperatorClient();

    self.bucketsRoot = '/' + self.client.user + '/buckets';
    self.testBucketPath = self.bucketsRoot + '/testBucket';
    self.type = 'bucket';

    self.createBucket = function create(p, headers, cb) {
        onCreateBucket.call(self, p, headers, self.client, cb);
    };

    self.deleteBucket = function del(p, headers, cb) {
        onDeleteBucket.call(self, p, headers, self.client, cb);
    };

    self.getBucket = function get(p, headers, cb) {
        onGetBucket.call(self, p, headers, self.client, cb);
    };

    self.listBuckets = function list(p, headers, cb) {
        onListBuckets.call(self, p, headers, self.client, cb);
    };

    var opts = {
        type: self.type
    };

    self.client.mkdir(self.testBucketPath, opts, function (err) {
        if (err) {
            next(err);
        } else {
            next(null);
        }
    });

    next();
}

function cleanupBucketTester(next) {
    var self = this;

    function closeClients(cb) {
        this.client.close();
        this.userClient.close();
        cb();
    }

    var opts = {
        type: self.type
    };

    self.client.rmr(self.testBucketPath, opts, function () {
        closeClients.call(self, next);
    });
    next();
}

function onCreateBucket(p, h, client, cb) {
    var self = this;

    assert.object(self);
    assert.object(client);
    assert.func(cb);

    var opts = {
        headers: h,
        path: p
    };

    client.signRequest({
        headers: opts.headers
    }, function (err) {
        if (err) {
            cb(err);
        } else {
            var body = {};
            if (p) {
                body.objectPath = p;
            }
            if (h) {
                body.headers = h;
            }

            self.client.jsonClient.put(opts, body,
            function (err2, req, res, o) {
                if (err2) {
                    cb(err2);
                } else {
                    if (!o) {
                        cb(err2);
                    } else {
                        cb(null, res);
                    }
                }
            });
        }
    });
}

function onDeleteBucket(p, h, client, cb) {
    var self = this;

    assert.object(self);
    assert.object(client);
    assert.func(cb);

    var opts = {
        headers: h,
        path: p
    };

    client.signRequest({
        headers: opts.headers
    }, function (err) {
        if (err) {
            cb(err);
        } else {
            var body = {};
            if (p) {
                body.objectPath = p;
            }
            if (h) {
                body.headers = h;
            }

            self.operatorClient.unlink(body.objectPath, opts,
            function (err2, res) {
                if (err2) {
                    cb(err2);
                } else {
                    if (!res) {
                        cb(err2);
                    } else {
                        cb(null, res);
                    }
                }
            });
        }
    });
}

function onGetBucket(p, h, client, cb) {
    var self = this;

    assert.object(self);
    assert.object(client);
    assert.func(cb);

    var opts = {
        headers: h,
        path: p
    };

    client.signRequest({
        headers: opts.headers
    }, function (err) {
        if (err) {
            cb(err);
        } else {
            var body = {};
            if (p) {
                body.objectPath = p;
            }
            if (h) {
                body.headers = h;
            }

            self.client.get(body.objectPath, opts,
            function (err2, stream, res) {
                if (err2 || !res) {
                    cb(err2);
                } else {
                    cb(null, stream, res);
                }
            });
        }
    });
}

function onListBuckets(p, h, client, cb) {
    var self = this;

    assert.object(self);
    assert.object(client);
    assert.func(cb);

    var opts = {
        headers: h,
        path: p
    };

    client.signRequest({
        headers: opts.headers
    }, function (err) {
        if (err) {
            cb(err);
        } else {
            var body = {};
            if (p) {
                body.folderPath = p;
            }
            if (h) {
                body.headers = h;
            }

            self.client.ls(body.folderPath, opts,
            function (err2, res) {
                if (err2) {
                    cb(err2);
                } else {
                    if (!res) {
                        cb(err2);
                    } else {
                        cb(null, res);
                    }
                }
            });
        }
    });
}

// Helper taken from MPU tests
function ifErr(t, err, desc) {
    t.ifError(err, desc);
    if (err) {
        t.deepEqual(err.body, {}, desc + ': error body');
        return (true);
    }

    return (false);
}

module.exports = {
    cleanupBucketTester: cleanupBucketTester,
    ifErr: ifErr,
    initBucketTester: initBucketTester
};
