/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

//
// A wrapper for a Boray client connection
//

var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var jsprim = require('jsprim');
var path = require('path');
var util = require('util');

var assert = require('assert-plus');
var backoff = require('backoff');
var boray = require('boray');
var once = require('once');
var vasync = require('vasync');
var VError = require('verror');

var utils = require('../utils');


///--- API

function Boray(options) {
    var self = this;

    EventEmitter.call(this);

    this.client = null;

    if (options.hasOwnProperty('borayOptions')) {
        this.borayOptions = jsprim.deepCopy(options.borayOptions);
    } else {
        this.borayOptions = {
            'host': options.host,
            'port': parseInt(options.port || 2020, 10),
            'retry': options.retry,
            'connectTimeout': options.connectTimeout || 1000
        };
    }

    this.log = options.log.child({ component: 'BorayClient' }, true);
    this.borayOptions.log = this.log;
    this.borayOptions.unwrapErrors = true;
    this.initBarrier = vasync.barrier();

    /*
     * Configure the exponential backoff object we use to manage backoff during
     * initialization.
     */
    this.initBackoff = new backoff.exponential({
        'randomisationFactor': 0.5,
        'initialDelay': 1000,
        'maxDelay': 300000
    });

    this.initBackoff.on('backoff', function (which, delay, error) {
        assert.equal(which + 1, self.initAttempts);
        self.log.warn({
            'nfailures': which + 1,
            'willRetryAfterMilliseconds': delay,
            'error': error
        }, 'Boray.initAttempt failed (will retry)');
    });

    this.initBackoff.on('ready', function () {
        this.initBarrier = vasync.barrier();
        this.initBarrier.start('initAttempt');
        self.initAttempt();
    });

    /*
     * Define event handlers for the Boray client used at various parts during
     * initialization.
     *
     * The Boray client should generally not emit errors, but it's known to do
     * so under some conditions.  Our response depends on what phases of
     * initialization we've already completed:
     *
     * (1) Before we've established a connection to the client: if an error is
     *     emitted at this phase, we assume that we failed to establish a
     *     connection and we abort the current initialization attempt.  We will
     *     end up retrying with exponential backoff.
     *
     * (2) After we've established a connection, but before initialization has
     *     completed: if an error is emitted at this phase, we'll log it but
     *     otherwise ignore it because we assume that whatever operations we
     *     have outstanding will also fail.
     *
     * (3) After we've initialized, errors are passed through to our consumer.
     */
    this.onErrorDuringInit = function onErrorDuringInit(err) {
        self.log.warn(err, 'ignoring client-level error during init');
    };
    this.onErrorPostInit = function onErrorPostInit(err) {
        self.log.warn(err, 'boray client error');
        self.emit('error', err);
    };

    /* These fields exist only for debugging. */
    this.initAttempts = 0;


    this.initBarrier.start('initAttempt');
    this.initAttempt();
}

util.inherits(Boray, EventEmitter);

Boray.prototype.initAttempt = function initAttempt() {
    var self = this;
    var log = this.log;

    assert.ok(this.client === null, 'previous initAttempt did not complete');

    this.initAttempts++;
    log.debug({
        'attempt': this.initAttempts
    }, 'Boray.initAttempt: entered');

    /*
     * Define vasync waterfall steps such that we can
     * decide which ones to add to the waterfall depending
     * on whether or not this is a read-only client.
     */
    self.client = boray.createClient(self.borayOptions);

    var onErrorDuringConnect = function onErrDuringConnect(err) {
        self.client = null;
        err = new VError(err, 'Boray.initAttempt');
        self.initBackoff.backoff(err);
    };

    self.client.on('error', onErrorDuringConnect);
    self.client.once('connect', function onConnect() {
        self.client.removeListener('error', onErrorDuringConnect);

        /*
         * We could reset the "backoff" object in the success case, or
         * even null it out since we're never going to use it again.
         * But it's not that large, and it may be useful for debugging,
         * so we just leave it alone.
         */
        self.client.on('error', self.onErrorPostInit);
        self.client.on('close', self.emit.bind(self, 'close'));
        self.client.on('connect', self.emit.bind(self, 'connect'));
        log.info({ 'attempt': self.initAttempts },
                 'Boray.initAttempt: done');
        self.emit('connect');
        self.initBarrier.done('initAttempt');
    });
};


///--- Exports

module.exports = {
    createClient: function createClient(opts) {
        return (new Boray(opts));
    }
};
