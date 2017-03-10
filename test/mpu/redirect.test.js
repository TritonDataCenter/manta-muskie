/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var manta = require('manta');
var path = require('path');
var MemoryStream = require('stream').PassThrough;
var uuid = require('node-uuid');
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
var createUpload = helper.createUpload;
var redirectPath = helper.redirectPath;
var uploadPath = helper.uploadPath;


before(function (cb) {
    var self = this;

    self.client = testHelper.createClient();
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
    this.client.rmr(this.dir, cb.bind(null, null));
});


// Redirect

test('redirect upload: GET /:account/uploads/:id', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };

        self.client.get(redirectPath(a, o.id), opts,
        function (err2, stream, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            t.checkResponse(res, 301);
            t.equal(res.headers.location, uploadPath(a, o.id));
            t.end();
        });
    });
});


test('redirect upload: PUT /:account/uploads/:id', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };

        var s = new MemoryStream();
        self.client.put(redirectPath(a, o.id), s, opts, function (err2, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            t.checkResponse(res, 301);
            t.equal(res.headers.location, uploadPath(a, o.id));
            t.end();
        });
    });
});


test('redirect upload: HEAD /:account/uploads/:id', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };

        self.client.info(redirectPath(a, o.id), opts, function (err2, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            // info() doesn't return a status code, but if the location is in
            // location header, the redirect was successful.
            t.equal(res.headers.location, uploadPath(a, o.id));
            t.end();
        });
    });
});


test('redirect upload: POST /:account/uploads/:id', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var options = manta.createOptions({
            contentType: 'application/json',
            accept: 'application/json',
            path: redirectPath(a, o.id)
        }, {});

        self.client.signRequest({
            headers: options.headers
        },
        function (err2) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            self.client.jsonClient.post(options, {}, function (err3, _, res) {
                if (ifErr(t, err3, 'redirect upload')) {
                    t.end();
                    return;
                }

                t.checkResponse(res, 301);
                t.equal(res.headers.location, uploadPath(a, o.id));
                t.end();
            });
        });
    });
});


test('redirect upload: DELETE /:account/uploads/:id', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };

        self.client.unlink(redirectPath(a, o.id), opts, function (err2, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            t.checkResponse(res, 301);
            t.equal(res.headers.location, uploadPath(a, o.id));
            t.end();
        });
    });
});


test('redirect upload: GET /:account/uploads/:id/:partNum', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };

        var pn = 0;
        self.client.get(redirectPath(a, o.id, pn), opts,
        function (err2, stream, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            t.checkResponse(res, 301);
            t.equal(res.headers.location, uploadPath(a, o.id, pn));
            t.end();
        });
    });
});


test('redirect upload: PUT /:account/uploads/:id/:partNum', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };

        var s = new MemoryStream();
        var pn = 0;
        self.client.put(redirectPath(a, o.id, pn), s, opts,
        function (err2, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            t.checkResponse(res, 301);
            t.equal(res.headers.location, uploadPath(a, o.id, pn));
            t.end();
        });
    });
});


test('redirect upload: HEAD /:account/uploads/:id/:partNum', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };

        var pn = 0;
        self.client.info(redirectPath(a, o.id, pn), opts, function (err2, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            // info() doesn't return a status code, but if the location is in
            // location header, the redirect was successful.
            t.equal(res.headers.location, uploadPath(a, o.id, pn));
            t.end();
        });
    });
});


test('redirect upload: POST /:account/uploads/:id/:partNum', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var pn = 0;
        var options = manta.createOptions({
            contentType: 'application/json',
            accept: 'application/json',
            path: redirectPath(a, o.id, pn)
        }, {});

        self.client.signRequest({
            headers: options.headers
        },
        function (err2) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            self.client.jsonClient.post(options, {}, function (err3, _, res) {
                if (ifErr(t, err3, 'redirect upload')) {
                    t.end();
                    return;
                }

                t.checkResponse(res, 301);
                t.equal(res.headers.location, uploadPath(a, o.id, pn));
                t.end();
            });
        });
    });
});


test('redirect upload: DELETE /:account/uploads/:id/:partNum', function (t) {
    var self = this;
    var a = self.client.user;
    var p = self.dir;

    createUpload(self, a, p, null, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        checkCreateResponse(t, o);
        var opts = {
            account: a
        };

        var pn = 0;
        self.client.unlink(redirectPath(a, o.id, pn), opts,
        function (err2, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            t.checkResponse(res, 301);
            t.equal(res.headers.location, uploadPath(a, o.id, pn));
            t.end();
        });
    });
});


test('redirect upload: non-existent id', function (t) {
    var self = this;
    var a = self.client.user;

    var bogus = uuid.v4();
    var opts = {
        account: a
    };


    self.client.get(redirectPath(a, bogus), opts, function (err, _, res) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }

        t.ok(verror.hasCauseWithName(err, 'ResourceNotFoundError'), err);
        t.checkResponse(res, 404);
        t.end();
    });
});
