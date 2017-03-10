/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var uuid = require('node-uuid');
var path = require('path');
var vasync = require('vasync');
var verror = require('verror');

if (require.cache[path.join(__dirname, '/../helper.js')])
    delete require.cache[path.join(__dirname, '/../helper.js')];
if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var testHelper = require('../helper.js');
var helper = require('./helper.js');

var after = testHelper.after;
var before = testHelper.before;
var test = testHelper.test;

var ifErr = helper.ifErr;
var checkCreateResponse = helper.checkCreateResponse;
var computePartsMD5 = helper.computePartsMD5;
var createPartOptions = helper.createPartOptions;
var createUpload = helper.createUpload;
var createUploadSubuser = helper.createUploadSubuser;
var redirectPath = helper.redirectPath;
var sanityCheckUpload = helper.sanityCheckUpload;
var writeObject = helper.writeObject;

before(function (cb) {
    var self = this;

    self.client = testHelper.createClient();
    self.userClient = testHelper.createUserClient('muskie_test_user');
    self.uploads_root = '/' + self.client.user + '/uploads';
    self.root = '/' + self.client.user + '/stor';
    self.dir = self.root + '/' + uuid.v4();

    self.client.mkdir(self.dir, function (mkdir_err) {
        if (mkdir_err) {
            cb(mkdir_err);
            return;
        } else {
            cb(null);
        }
    });
});


after(function (cb) {
    var self = this;
    self.client.close();
    self.userClient.close();
    cb();
});



// Subusers (not supported for MPU API)

// Create
test('subusers disallowed: create', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;
    var h = {};

    createUploadSubuser(self, a, p, h, function (err) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }
        t.ok(verror.hasCauseWithName(err,
            'AuthorizationFailedError'), err);
        t.end();
    });
});

// Get
test('subusers disallowed: get upload under same account', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;
    var h = {};

    createUpload(self, a, p, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };
        self.userClient.getUpload(o.id, opts, function (err2, upload) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2, 'AuthorizationFailedError'),
                err2);
            t.end();
        });
    });
});


// Upload
test('subusers disallowed: upload part', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;
    var h = {};

    createUpload(self, a, p, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);

        var pn = helper.randomPartNum();
        var opts = createPartOptions(a, helper.TEXT);

        writeObject(self.userClient, o.id, pn, opts, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2, 'NoMatchingRoleTagError'),
                err2);
            t.end();
        });
    });
});


// Abort
test('subusers disallowed: abort', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;
    var h = {};

    createUpload(self, a, p, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };

        self.userClient.abortUpload(o.id, opts, function (err2) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2, 'AuthorizationFailedError'),
                err2);
            t.end();
        });
    });
});


// Commit
test('subusers disallowed: commit', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;
    var h = {};

    createUpload(self, a, p, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };

        self.userClient.commitUpload(o.id, [], opts, function (err2) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2, 'AuthorizationFailedError'),
                err2);
            t.end();
        });
    });
});


// Redirect
test('subusers disallowed: redirect (GET /:account/uploads/id)', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;
    var h = {};

    createUpload(self, a, p, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };

        self.userClient.get(redirectPath(a, o.id), opts, function (err2) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2, 'AuthorizationFailedError'),
                err2);
            t.end();
        });
    });
});


test('subusers disallowed: redirect (GET /:account/uploads/id)', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;
    var h = {};

    createUpload(self, a, p, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };

        var pn = 0;

        self.userClient.get(redirectPath(a, o.id, pn), opts, function (err2) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2, 'AuthorizationFailedError'),
                err2);
            t.end();
        });
    });
});
