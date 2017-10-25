/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var path = require('path');
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


before(function (cb) {
    helper.initMPUTester.call(this, cb);
});


after(function (cb) {
    helper.cleanupMPUTester.call(this, cb);
});


// Delete parts/upload directories: allowed cases

test('del upload directory with operator override', function (t) {
    var self = this;

    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            query: {
                allowMpuDeletes: true
            }
        };
        self.operatorClient.unlink(self.uploadPath(), opts,
        function (err2, res) {
            if (ifErr(t, err2, 'unlink')) {
                t.end();
                return;
            }

            t.ok(res);
            t.checkResponse(res, 204);
            t.end();
        });
    });
});


test('del part with operator override', function (t) {
    var self = this;

    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.MIN_PART_NUM;
        self.writeTestObject(self.uploadId, pn, function (err2, _) {
            if (ifErr(t, err, 'uploaded part')) {
                t.end();
                return;
            }

            var opts = {
                query: {
                    allowMpuDeletes: true
                }
            };
            self.operatorClient.unlink(self.uploadPath(pn), opts,
            function (err3, res) {
                if (ifErr(t, err3, 'unlink')) {
                    t.end();
                    return;
                }

                t.ok(res);
                t.checkResponse(res, 204);
                t.end();
            });
        });
    });
});


// Delete parts/upload directories: operator, no override provided

test('del upload directory: operator but no override', function (t) {
    var self = this;

    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        self.operatorClient.unlink(self.uploadPath(), function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2,
                'UnprocessableEntityError'), err2);
            t.checkResponse(res, 422);
            t.end();
        });
    });
});


test('del part: operator but no override', function (t) {
    var self = this;

    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.MIN_PART_NUM;
        self.writeTestObject(self.uploadId, pn, function (err2, _) {
            if (ifErr(t, err, 'uploaded part')) {
                t.end();
                return;
            }

            self.operatorClient.unlink(self.uploadPath(pn),
            function (err3, res) {
                t.ok(err3);
                if (!err3) {
                    return (t.end());
                }
                t.ok(verror.hasCauseWithName(err3,
                    'UnprocessableEntityError'), err3);
                t.checkResponse(res, 422);
                t.end();
            });
        });
    });
});


// Delete parts/upload directories: non-operator, override provided

test('del upload directory: non-operator with override', function (t) {
    var self = this;
    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            query: {
                allowMpuDeletes: true
            }
        };

        self.client.unlink(self.uploadPath(), opts, function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2,
                'MethodNotAllowedError'), err2);
            t.checkResponse(res, 405);
            t.end();
        });
    });
});


test('del part: non-operator with override', function (t) {
    var self = this;
    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.MIN_PART_NUM;
        self.writeTestObject(self.uploadId, pn, function (err2, _) {
            if (ifErr(t, err2, 'uploaded part')) {
                t.end();
                return;
            }

            var opts = {
                query: {
                    allowMpuDeletes: true
                }
            };

            self.client.unlink(self.uploadPath(pn), opts, function (err3, res) {
                t.ok(err3);
                if (!err3) {
                    return (t.end());
                }
                t.ok(verror.hasCauseWithName(err3,
                    'MethodNotAllowedError'), err3);
                t.checkResponse(res, 405);
                t.end();
            });
        });
    });
});


// Delete parts/upload directories: operator, but query param is not `true`

test('del upload directory: operator but query param is false', function (t) {
    var self = this;

    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            query: {
                allowMpuDeletes: false
            }
        };

        self.operatorClient.unlink(self.uploadPath(), opts,
        function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2,
                'UnprocessableEntityError'), err2);
            t.checkResponse(res, 422);
            t.end();
        });
    });
});


test('del part: operator but query param is false', function (t) {
    var self = this;

    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.MIN_PART_NUM;
        self.writeTestObject(self.uploadId, pn, function (err2, _) {
            if (ifErr(t, err, 'uploaded part')) {
                t.end();
                return;
            }

            var opts = {
                query: {
                    allowMpuDeletes: false
                }
            };

            self.operatorClient.unlink(self.uploadPath(pn), opts,
            function (err3, res) {
                t.ok(err3);
                if (!err3) {
                    return (t.end());
                }
                t.ok(verror.hasCauseWithName(err3,
                    'UnprocessableEntityError'), err3);
                t.checkResponse(res, 422);
                t.end();
            });
        });
    });
});


test('del upload directory: operator but query param is not bool',
function (t) {
    var self = this;

    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var opts = {
            query: {
                allowMpuDeletes: 1
            }
        };

        self.operatorClient.unlink(self.uploadPath(), opts,
        function (err2, res) {
            t.ok(err2);
            if (!err2) {
                return (t.end());
            }
            t.ok(verror.hasCauseWithName(err2,
                'UnprocessableEntityError'), err2);
            t.checkResponse(res, 422);
            t.end();
        });
    });
});


test('del part: operator but query param is not bool', function (t) {
    var self = this;

    var h = {};
    self.createUpload(self.path, h, function (err, o) {
        if (ifErr(t, err, 'created upload')) {
            t.end();
            return;
        }

        var pn = helper.MIN_PART_NUM;
        self.writeTestObject(self.uploadId, pn, function (err2, _) {
            if (ifErr(t, err, 'uploaded part')) {
                t.end();
                return;
            }

            var opts = {
                query: {
                    allowMpuDeletes: false
                }
            };

            self.operatorClient.unlink(self.uploadPath(pn), opts,
            function (err3, res) {
                t.ok(err3);
                if (!err3) {
                    return (t.end());
                }
                t.ok(verror.hasCauseWithName(err3,
                    'UnprocessableEntityError'), err3);
                t.checkResponse(res, 422);
                t.end();
            });
        });
    });
});
