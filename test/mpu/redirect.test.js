/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var manta = require('manta');
var path = require('path');
var MemoryStream = require('stream').PassThrough;
var uuidv4 = require('uuid/v4');
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


var mpuEnabled = Boolean(require('../../etc/config.json').enableMPU);
if (mpuEnabled) {

before(function (cb) {
    helper.initMPUTester.call(this, cb);
});


after(function (cb) {
    helper.cleanupMPUTester.call(this, cb);
});


// Redirect

test('redirect upload: GET /:account/uploads/:id', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            account: self.client.user
        };
        self.client.get(self.redirectPath(), opts,
        function (err2, stream, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            t.checkResponse(res, 301);
            t.equal(res.headers.location, self.uploadPath());
            t.end();
        });
    });
});


test('redirect upload: PUT /:account/uploads/:id', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            account: self.client.user
        };

        var s = new MemoryStream();
        self.client.put(self.redirectPath(), s, opts, function (err2, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            t.checkResponse(res, 301);
            t.equal(res.headers.location, self.uploadPath());
            t.end();
        });
    });
});


test('redirect upload: HEAD /:account/uploads/:id', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            account: self.client.user
        };
        self.client.info(self.redirectPath(), opts, function (err2, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            // info() doesn't return a status code, but if the location is in
            // location header, the redirect was successful.
            t.equal(res.headers.location, self.uploadPath());
            t.end();
        });
    });
});


test('redirect upload: POST /:account/uploads/:id', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var options = {
            headers: {
                'content-type': 'application/json',
                'accept': 'application/json'
            },
            path: self.redirectPath()
        };

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
                t.equal(res.headers.location, self.uploadPath());
                t.end();
            });
        });
    });
});


test('redirect upload: DELETE /:account/uploads/:id', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            account: self.client.user
        };
        self.client.unlink(self.redirectPath(), opts, function (err2, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            t.checkResponse(res, 301);
            t.equal(res.headers.location, self.uploadPath());
            t.end();
        });
    });
});


test('redirect upload: GET /:account/uploads/:id/:partNum', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            account: self.client.user
        };

        var pn = 0;
        self.client.get(self.redirectPath(pn), opts,
        function (err2, stream, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            t.checkResponse(res, 301);
            t.equal(res.headers.location, self.uploadPath(pn));
            t.end();
        });
    });
});


test('redirect upload: PUT /:account/uploads/:id/:partNum', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            account: self.client.user
        };
        var s = new MemoryStream();
        var pn = 0;
        self.client.put(self.redirectPath(pn), s, opts, function (err2, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            t.checkResponse(res, 301);
            t.equal(res.headers.location, self.uploadPath(pn));
            t.end();
        });
    });
});


test('redirect upload: HEAD /:account/uploads/:id/:partNum', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            account: self.client.user
        };
        var pn = 0;
        self.client.info(self.redirectPath(pn), opts, function (err2, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            // info() doesn't return a status code, but if the location is in
            // location header, the redirect was successful.
            t.equal(res.headers.location, self.uploadPath(pn));
            t.end();
        });
    });
});


test('redirect upload: POST /:account/uploads/:id/:partNum', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = 0;
        var options = {
            headers: {
                'content-type': 'application/json',
                'accept': 'application/json'
            },
            path: self.redirectPath(pn)
        };

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
                t.equal(res.headers.location, self.uploadPath(pn));
                t.end();
            });
        });
    });
});


test('redirect upload: DELETE /:account/uploads/:id/:partNum', function (t) {
    var self = this;
    self.createUpload(self.path, null, function (err) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            account: self.client.user
        };
        var pn = 0;
        self.client.unlink(self.redirectPath(pn), opts, function (err2, res) {
            if (ifErr(t, err2, 'redirect upload')) {
                t.end();
                return;
            }

            t.checkResponse(res, 301);
            t.equal(res.headers.location, self.uploadPath(pn));
            t.end();
        });
    });
});


test('redirect upload: non-existent id', function (t) {
    var self = this;
    var bogus = uuidv4();
    var opts = {
        account: self.client.user
    };

    self.client.get('/' + self.client.user + '/uploads/' + bogus, opts,
    function (err, _, res) {
        t.ok(err);
        if (!err) {
            return (t.end());
        }

        t.ok(verror.hasCauseWithName(err, 'ResourceNotFoundError'), err);
        t.checkResponse(res, 404);
        t.end();
    });
});

} // mpuEnabled
