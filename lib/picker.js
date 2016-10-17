/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

//
// Picker is the component that selects where to send data on PUT requests.
//
// The only method public available from picker is `choose`.  Choose takes
// a desired number of replicas and a size (in bytes), and then selects two
// random "tuples" (the number of items in a tuple is #replicas).  The first
// random tuple is "primary," and then we have a backup tuple.  The contract
// here is that upstack code tries all hosts in "primary," and if all are up
// we're good to go; if any fail it falls through to trying all hosts in
// "secondary." While not the most sophisticated and/or error-proof approach,
// this is simple to reason about, and should be "good enough," given what we
// know about our infrastructure (i.e., we expect it to be up).
//
// So in terms of implementation, Picker periodically refreshes a (sorted) set
// of servers per datacenter that is advertised in a moray bucket
// (manta_storage).  To see how data gets in manta_storage, see minnow.git.
//
// So conceptually it looks like this:
//
// {
//   us-east-1: [a, b, c, ...],
//   us-east-2: [d, e, f, ...],
//   us-east-3: [g, h, i, ...],
//   ...
// }
//
// Where the objects `a...N` are the full JSON representation of a single mako
// instance.  In that object, we really only care about two fields:
//
//   -- manta_storage_id (hostname)
//   -- availableMB
//
// We keep those sets sorted by `availableMB`, and everytime choose is run, we
// make a "view" of the set for each data center that tells us all the servers
// that have that amount of storage and larger (binary search).
//
// Once we have that "view," we simply pick random nodes from the set(s).
// Lastly, we RR across DCs so we spread objects around evenly.

var EventEmitter = require('events').EventEmitter;
var util = require('util');

var moray = require('moray');

var assert = require('assert-plus');
var once = require('once');


require('./errors');


///--- Globals

var sprintf = util.format;



///--- Private Functions

// Refreshes the local cache from moray

var fetch = function fetch_moray(opts, cb) {
    assert.object(opts, 'options');
    assert.number(opts.lag, 'options.lag');
    assert.object(opts.moray, 'options.moray');
    assert.optionalNumber(opts.limit, 'options.limit');
    assert.optionalNumber(opts.marker, 'options.marker');
    assert.optionalNumber(opts.utilization, 'options.utilization');
    assert.optionalArrayOfObject(opts.values, 'options.values');
    assert.func(cb, 'callback');

    cb = once(cb);

    var count = 0;
    var f = sprintf('(&(percentUsed<=%d)(timestamp>=%d)%s)',
                    opts.utilization || 90,
                    Date.now() - opts.lag,
                    opts.marker ? '(_id>=' + opts.marker + ')' : '');
    var marker = opts.marker;
    var _opts = {
        limit: opts.limit || 100,
        sort: {
            attribute: '_id',
            order: 'ASC'
        }
    };
    var req = opts.moray.findObjects('manta_storage', f, _opts);
    var values = opts.values || [];

    req.once('error', cb);

    req.on('record', function onRecord(data) {
        values.push(data.value);
        count = data._count;
        marker = data._id;
    });

    req.once('end', function () {
        if (values.length < count) {
            var next = {
                lag: opts.lag,
                limit: opts.limit,
                marker: ++marker,
                moray: opts.moray,
                utilization: opts.utilization,
                values: values
            };
            fetch(next, cb);
        } else {
            cb(null, values);
        }
    });
};


var _cached_stub_data;
function fetch_stub(opts, cb) {
    cb = once(cb);

    var fs = require('fs');
    var path = require('path');

    var fname = process.env.UNIT_TEST_STUB_NAME || 'picker.stub.json';
    var file = path.join(__dirname, '..', 'test', fname);
    var _opts = {
        encoding: 'utf8'
    };

    if (_cached_stub_data) {
        process.nextTick(function () {
            cb(null, _cached_stub_data);
        });
    } else {
        fs.readFile(file, _opts, function (err, data) {
            if (err) {
                cb(err);
                return;
            }

            var values;
            try {
                values = JSON.parse(data).filter(function (obj) {
                    return (obj.value.percentUsed < (opts.utilization || 90));
                }).map(function (obj) {
                    return (obj.value);
                });
            } catch (e) {
                cb(e);
                return;
            }

            _cached_stub_data = values;
            cb(null, values);
        });
    }
}



// "this" must be bound to an instance of Picker. This simply
// manages timers and calls `fetch`, above.
function poll() {
    var opts = {
        lag: this.lag,
        moray: this.client
    };
    var self = this;

    function reschedule() {
        clearTimeout(self._timer);
        self._timer = setTimeout(poll.bind(self), self.interval);
    }

    clearTimeout(self._timer);
    fetch(opts, function (err, values) {
        if (err) {
            self.emit('error', err);
            return;
        }

        var obj = {};
        values.forEach(function (v) {
            if (!obj[v.datacenter])
                obj[v.datacenter] = [];
            obj[v.datacenter].push(v);
        });

        // We just defer to the next tick so we're not tying
        // up the event loop to sort a lot if the list is large
        process.nextTick(function () {
            var count = 0;
            var dcs = Object.keys(obj);

            dcs.forEach(function (k) {
                obj[k].sort(function (a, b) {
                    if (a.availableMB < b.availableMB)
                        return (-1);
                    if (a.availableMB > b.availableMB)
                        return (1);
                    return (0);
                });
                count++;
            });

            // Don't replace if we got an empty "DB"
            if (count > 0) {
                self.datacenters = dcs;
                self.db = obj;
                self.emit('topology', self.db);
            } else {
                self.log.warn('Picker::poll: could not find any minnow ' +
                    'instances');
            }

            reschedule();
        });
    });
}


// Just picks a random number, and optionally skips the last one we saw
function random(min, max, skip) {
    var num = (Math.floor(Math.random() * (max - min + 1)) + min);

    if (num === skip)
        num = ((num + 1) % max);

    return (num);
}


// Fisher-Yates shuffle - courtesy of http://bost.ocks.org/mike/shuffle/
function shuffle(array) {
    var m = array.length, t, i;
    while (m) {
        i = Math.floor(Math.random() * m--);
        t = array[m];
        array[m] = array[i];
        array[i] = t;
    }
    return (array);
}


// Modified binary-search. we're looking for the point in the set where all
// servers have >= the desired space.  Logically you would then do
//
// set.slice(lower_bound(set, 100));
//
// But that creates a copy - but really the return value of this to $end is
// what the picker logic can then look at
function lower_bound(set, size, low, high) {
    assert.arrayOfObject(set, 'set');
    assert.number(size);

    low = low || 0;
    high = high || set.length;

    while (low < high) {
        var mid = Math.floor(low + (high - low) / 2);
        if (set[mid].availableMB >= size) {
            high = mid;
        } else {
            low = mid + 1;
        }
    }

    if (!set[low] || set[low].availableMB < size)
        low = -1;

    return (low);
}



///--- API

/**
 * Creates an instance of picker, and an underlying moray client.
 *
 * You can pass in all the usual moray-client options, and additionally pass in
 * an `interval` field, which indicates how often to go poll Moray for minnow
 * updates.  The default is 30s.  Additionally, you can pass in a `lag` field,
 * which indicates how much "staleness" to allow in Moray records. The default
 * for `lag` is 60s.
 */
function Picker(opts) {
    assert.object(opts, 'options');
    assert.optionalBool(opts.multiDC, 'options.multiDC');
    assert.optionalNumber(opts.connectTimeout, 'options.connectTimeout');
    assert.optionalNumber(opts.interval, 'options.interval');
    assert.optionalNumber(opts.lag, 'options.lag');
    assert.object(opts.log, 'options.log');
    assert.string(opts.url, 'options.url');
    // When a streaming upload comes into Manta, there is no content-length
    // associated with the request so it uses the DEF_MAX_LEN for size.  That is
    // the same size the Picker will use when looking for sharks.  In smaller
    // deployments (coal, for example), storage zones will not have enough space
    // for DEF_MAX_LEN, causing all streaming requests to fail.  So this should
    // *only* be set to true for small deployments.
    assert.optionalBool(opts.ignoreSize, 'options.ignoreSize');

    EventEmitter.call(this);
    var self = this;

    this.client = moray.createClient({
        unwrapErrors: true,
        log: opts.log,
        connectTimeout: opts.connectTimeout,
        url: opts.url
    });
    this.connectTimeout = opts.connectTimeout || 1000;
    this.db = null;
    this.dcIndex = -1;
    this.interval = parseInt(opts.interval || 30000, 10);
    this.lag = parseInt(opts.lag || (60 * 60 * 1000), 10);
    this.log = opts.log.child({component: 'picker'}, true);
    this.multiDC = opts.multiDC === undefined ? true : opts.multiDC;
    this.url = opts.url;
    this.ignoreSize = opts.ignoreSize === undefined ? false : opts.ignoreSize;

    this.client.on('close', function () {
        clearTimeout(self._timer);
        self.emit('close');
    });

    this.client.once('connect', poll.bind(this));

    this.client.on('error', function (err) {
        self.log.error(err, 'picker: unexpected error: %s',
                       err.toString());
        self.emit('error', err);
    });

    this.once('topology', this.emit.bind(this, 'connect'));
}
util.inherits(Picker, EventEmitter);


Picker.prototype.close = function close() {
    clearTimeout(this._timer);
    if (this.client)
        this.client.close();
};


/**
 * Selects N shark nodes from sharks with more space than the request length.
 *
 * @param {object} options -
 *                   - {number} size => req.getContentLength()
 *                   - {string} requestId => req.getId()
 *                   - {number} replicas => req.header('x-durability-level')
 * @param {funtion} callback => f(err, [sharkClient])
 */
Picker.prototype.choose = function choose(opts, cb) {
    assert.object(opts, 'options');
    assert.optionalObject(opts.log, 'options.log');
    assert.optionalNumber(opts.replicas, 'options.replicas');
    assert.optionalNumber(opts.size, 'options.size');
    assert.func(cb, 'callback');

    cb = once(cb);

    var dcs = [];
    var log = opts.log || this.log;
    var offsets = [];
    var replicas = opts.replicas || 2;
    var seen = [];
    var self = this;
    var size = self.ignoreSize ? 1 :
        Math.ceil((opts.size || 0) / 1048576) || 5120;

    log.debug({
        replicas: replicas,
        size: size,
        ignoreSize: self.ignoreSize
    }, 'Picker::choose: entered');

    this.datacenters.forEach(function filterDatacenters(dc) {
        var l = lower_bound(self.db[dc], size);
        if (l !== -1) {
            dcs.push(dc);
            offsets.push(l);
        }
    });
    dcs = shuffle(dcs);

    if ((replicas > 1 && this.multiDC && dcs.length < 2) ||
        !dcs.length ||
        (replicas > 1 && dcs.some(function (dc) {
            return (!dc.length);
        }))) {
        log.warn('Picker::choose: not enough DCs available');
        cb(new NotEnoughSpaceError(size));
        return;
    }

    function host() {
        if (++self.dcIndex >= dcs.length)
            self.dcIndex = 0;

        var ndx = self.dcIndex;
        var dc = self.db[dcs[ndx]];
        var s = random(offsets[ndx], dc.length - 1);

        if (seen.indexOf(dc[s].manta_storage_id) === -1) {
            seen.push(dc[s].manta_storage_id);
        } else {
            var start = s;
            do {
                if (++s === dc.length)
                    s = offsets[ndx];

                if (s === start) {
                    log.debug({
                        datacenter: dcs[ndx]
                    }, 'Picker::choose: exhausted DC');
                    return (null);
                }

            } while (seen.indexOf(dc[s].manta_storage_id) !== -1);

            seen.push(dc[s].manta_storage_id);
        }

        return ({
            datacenter: dc[s].datacenter,
            manta_storage_id: dc[s].manta_storage_id
        });
    }

    function set() {
        var s = [];

        for (var j = 0; j < replicas; j++) {
            var _s = host();
            if (_s === null)
                return (null);
            s.push(_s);
        }

        return (s);
    }

    // We always pick three sets, and we pedantically ensure
    // that we've got them splayed x-dc
    var sharks = [];
    for (var i = 0; i < 3; i++) {
        var tuple = set();

        if (!sharks.length && (!tuple || tuple.length < replicas)) {
            cb(new NotEnoughSpaceError(size));
        } else if (tuple && this.multiDC && replicas > 1) {
            var _dcs = tuple.map(function (s) {
                return (s.datacenter);
            }).reduce(function (last, now) {
                if (last.indexOf(now) === -1)
                    last.push(now);
                return (last);
            }, []);

            if (_dcs.length < 2) {
                cb(new NotEnoughSpaceError(size));
                return;
            }
        }

        if (tuple)
            sharks.push(tuple);
    }

    log.debug({
        replicas: replicas,
        sharks: sharks,
        size: size
    }, 'picker::choose: done');
    cb(null, sharks);
};


Picker.prototype.toString = function toString() {
    var str = '[object Picker <';
    str += 'connectTimeout=' + this.connectTimeout + ', ';
    str += 'datacenters=' + this.datacenters.length + ', ';
    str += 'interval=' + this.interval + ', ';
    str += 'lag=' + this.lag + ', ';
    str += 'moray=' + this.client.toString();
    str += '>]';

    return (str);
};



///--- Exports

module.exports = {

    createClient: function createClient(options) {
        return (new Picker(options));
    }

};



///--- Tests

function test(N) {
    var picker = new Picker({
        log: require('bunyan').createLogger({
            level: process.env.LOG_LEVEL || 'info',
            name: 'picker_test',
            stream: process.stdout
        }),
        interval: 10,
        multiDC: true,
        url: 'tcp://10.99.99.44:2020'
    });

    var min = Infinity;
    var max = 0;
    var total = 0;
    var runs = 0;
    var dcs = {};
    var hosts = {};

    function report() {
        var keys = Object.keys(hosts);
        keys.sort();
        console.log('**** Host Selection ****');
        keys.forEach(function (k) {
            console.log(k + ' ' + hosts[k]);
        });
        console.log('\n**** Datacenter Selection ****');
        keys = Object.keys(dcs);
        keys.sort();
        keys.forEach(function (k) {
            console.log(k + ' ' + dcs[k]);
        });
        console.log('\n**** Timing ****');
        console.log('avg: ' + total / N  + 'ms');
        console.log('max: ' + max + 'ms');
        console.log('min: ' + min + 'ms');

        picker.close();
    }

    function select(t) {
        var start = new Date().getTime();
        picker.choose({}, function onChosen(err, sharks) {
            assert.ifError(err);
            var delta = new Date().getTime() - start;
            if (delta > max)
                max = delta;
            if (delta < min)
                min = delta;
            total += delta;

            Object.keys(sharks).forEach(function track(k) {
                var dc_names = [];
                sharks[k].forEach(function (s) {
                    var id = s.manta_storage_id;
                    if (!hosts[id])
                        hosts[id] = 0;

                    hosts[id]++;

                    dc_names.push(s.datacenter);
                });

                var dc_key = dc_names.join(' ');
                if (!dcs[dc_key])
                    dcs[dc_key] = 0;
                dcs[dc_key]++;
            });

            setImmediate(++runs < N ? select : report);
        });
    }

    picker.on('connect', select);
}


if (process.env.UNIT_TEST) {
    fetch = fetch_stub;
    test(parseInt(process.argv.length > 2 ? process.argv[2] : 10000, 10));
}
