/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var bunyan = require('bunyan');
var isValidBucketName = require('../lib/buckets/common.js').isValidBucketName;

exports.bucketName = function bucketName(t) {
    var validBucketNames = [
        // Simple names that start and end with a letter and/or number
        'abc',
        '123',
        'a12',
        '1ab',
        // Hyphens are allowed in the middle
        'a-b',
        '1-2',
        'a-1',
        '1-a',
        // Test multiple hyphens and more complex patterns
        'a--b',
        '1-2-3',
        'qw-90ert-y78-56uiop',
        // Names with periods
        'a.b',
        '1.a.2',
        'a-1.b-2.c-3',
        'qw-90.ert-y.78-56u.iop',
        // Names that sort of look like IP addresses but are valid
        '1.2',
        '1.2.3',
        '1.2.3.4.5',
        '1.2.a.4',
        '1234.5.6.7',
        '111.222.34.5555',
        // A name that is the maximum allowed length (63 chars)
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    ];

    var invalidBucketNames = [
        // Too short
        'a',
        'aa',
        // Too long (64 and 65 chars)
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        // Too long, even if broken up with dots -- total length is what counts
        'aaaaaaaaaaaa.aaaaaaaaaaa.aaaaaaaaaaaaa.aaaaaaaaaaaa.aaaaaaaaa.aa',
        // No capital letters
        'Hello',
        'HeLlO',
        'HELLO',
        // No punctuation other than dots or hyphens
        '!h!ello!',
        '*smile:.%)',
        'foo/bar',
        // No hyphens at the beginning or end (or both)
        '-aa',
        'aa-',
        '-a-',
        // Hyphen rules also apply to sections between dots
        'a.-bb.c',
        'a.bb-.c',
        'a.-b-.c',
        // No starting or ending with dots, or more than one dot in a row
        '.aa',
        'aa.',
        'a..a',
        'a...a',
        /*
         * Nothing that resembles an IP address -- that is, no names that are
         * four groupings of between one and three digits each. Whether or not
         * the numbers make sense as an IP address doesn't matter.
         */
        '1.2.3.4',
        '1.255.6.77',
        '127.0.0.1',
        '999.999.999.999'
    ];

    validBucketNames.forEach(function testValid(name) {
        t.ok(isValidBucketName(name));
    });

    invalidBucketNames.forEach(function testInvalid(name) {
        t.equal(isValidBucketName(name), false);
    });

    t.done();
};
