/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var mod_util = require('../lib/utils.js');
var jsc = require('jsverify');

///--- Constants

///--- Tests

/**
 * shuffleTest is a property test. jsverify generates 100 random
 * arrays, and checks that the property defined always holds. The
 * property this tests is that the mod_utils.shuffle function does not
 * add or remove elements from an array. It does not check that
 * shuffle actually shuffles, only that the contents of the original
 * array all remain present when shuffle.
 */
exports.shuffleTest = function (t) {
    var propRes =
        jsc.checkForall(jsc.nearray(jsc.nat), function propShuff(arr) {
            var prevLength = arr.length;
            // NOTE: copy the array so we can compare the original to the
            // shuffled, as shuffle shuffles in place
            var shuffled = mod_util.shuffle(arr.slice());

            if (shuffled.length === prevLength) {
                shuffled.sort();
                arr.sort();
                return (arr.reduce(function elementsEqual(acc, e, idx) {
                    return (acc && shuffled[idx] === e);
                }, true));
            } else {
                return (false);
            }
        });

    // use equals, as propRes is a report object on property failure,
    // but it's contents are logged by jsverify
    t.ok(propRes === true, 'Property:: shuffle maintains arr contents');
    t.done();
};
