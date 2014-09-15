/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var crypto = require('crypto');
var stream = require('stream');
var util = require('util');

var assert = require('assert-plus');

require('./errors');


///--- Helpers

function onTimeoutHandler() {
    this.emit('timeout');
}



///--- API

function CheckStream(opts) {
    assert.object(opts, 'options');
    assert.optionalString(opts.algorithm, 'options.algorithm');
    assert.number(opts.maxBytes, 'options.maxBytes');
    assert.number(opts.timeout, 'opts.timeout');

    stream.Writable.call(this, opts);

    var self = this;

    this.algorithm = opts.algorithm || 'md5';
    this.bytes = 0;
    this.hash = crypto.createHash(this.algorithm);
    this.maxBytes = opts.maxBytes;
    this.start = Date.now();
    this.timeout = opts.timeout;
    this.timer = setTimeout(onTimeoutHandler.bind(this), this.timeout);

    this.once('finish', function onFinish() {
        setImmediate(function () {
            if (!self._digest)
                self._digest = self.hash.digest('buffer');

            self.emit('done');
        });
    });
}
util.inherits(CheckStream, stream.Writable);
module.exports = CheckStream;


CheckStream.prototype.abandon = function abandon() {
    this._dead = true;
    clearTimeout(this.timer);
    this.removeAllListeners('error');
    this.removeAllListeners('finish');
    this.removeAllListeners('length_exceeded');
    this.removeAllListeners('timeout');
};


CheckStream.prototype.digest = function digest(encoding) {
    assert.optionalString(encoding, 'encoding');

    clearTimeout(this.timer);

    if (!this._digest)
        this._digest = this.hash.digest('buffer');

    var ret = this._digest;
    if (this._digest && encoding)
        ret = this._digest.toString(encoding);

    return (ret);
};


CheckStream.prototype._write = function _write(chunk, encoding, cb) {
    if (this._dead) {
        cb();
        return;
    }
    var self = this;

    clearTimeout(this.timer);
    this.hash.update(chunk, encoding);
    this.bytes += chunk.length;
    if (this.bytes > this.maxBytes) {
        this.emit('length_exceeded', this.bytes);
        setImmediate(function () {
            cb(self._dead ? null : new MaxSizeExceededError(self.maxBytes));
        });
    } else {
        this.timer = setTimeout(onTimeoutHandler.bind(this), this.timeout);
        cb();
    }
};


CheckStream.prototype.toString = function toString() {
    return ('[object CheckStream<' + this.algorithm + '>]');
};
