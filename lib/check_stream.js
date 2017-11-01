/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * CheckStream calculates the md5 hash of a data stream. In practice this is
 * used to calculate the md5 hash of objects as they are streamed to or from
 * sharks. The final hash is stored in Moray to be compared against when
 * objects are read back from sharks.
 *
 * As the name implies, this is implemented as a stream. As chunks of bytes flow
 * through the _write() function of the CheckStream they are added to the md5
 * hash. The final md5 hash can be retrieved using CheckStream.digest().
 *
 * A 'timeout' event is emitted by CheckStream if it goes too long without
 * receiving any data. Consumers of CheckStream use the 'timeout' event as a
 * trigger to abandon the CheckStream. The stream can be abandoned by calling
 * CheckStream.abandon().
 *
 * CheckStream ensures that the number of bytes streamed doesn't exceed what is
 * expected. The maximum bytes CheckStream will read is set by the 'maxBytes'
 * argument to the constructor.
 *
 * Throughput metrics are collected in the CheckStream using a node-artedi
 * collector. Depending on the 'counter' argument, either inbound or outbound
 * throughput is tracked.
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
    assert.object(opts.counter, 'opts.counter');

    stream.Writable.call(this, opts);

    var self = this;

    this.algorithm = opts.algorithm || 'md5';
    this.bytes = 0;
    this.hash = crypto.createHash(this.algorithm);
    this.maxBytes = opts.maxBytes;
    this.start = Date.now();
    this.timeout = opts.timeout;
    this.timer = setTimeout(onTimeoutHandler.bind(this), this.timeout);
    this.throughput_counter = opts.counter;

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
    this.throughput_counter.add(chunk.length);
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
