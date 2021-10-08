/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2021 Joyent, Inc.
 */

var assert = require('assert-plus');
var contentDisposition = require('content-disposition');
var VError = require('verror');

//
// Given a list of lists, this function will interleave each element
// from each list into a new flat list.
//
// For example:
// var a = [1,2,3];
// var b = [4,0,5,6];
// var c = [7,8];
// var list = interleave([a,b,c]);
//
// list will be:
// [ 1, 4, 7, 2, 0, 8, 3, 5, 6 ]
//
// This is basically just matrix transposed by column
//
function interleave(matrix) {
    var list = [];
    var maxCols = 0;

    (function next(col) {
        matrix.forEach(function (row) {
            if (col < row.length)
                list.push(row[col]);

            maxCols = Math.max(row.length, maxCols);
        });
        if (col < maxCols - 1)
            next(++col);
    })(0);

    return (list);
}


function shallowCopy(obj) {
    if (!obj)
        return (obj);

    var copy = {};
    Object.keys(obj).forEach(function (k) {
        copy[k] = obj[k];
    });
    return (copy);
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


/**
 * Validates, and canonicalizes a "content-disposition" header (in-place), if
 * present. If the value is invalid, an Error instance is returned. Otherwise,
 * null is returned.
 */
function canonicalizeContentDisposition(headers) {
    assert.object(headers, 'headers');

    var canonicalizedVal;
    var headerVal;
    var parsedVal;

    if (headers['content-disposition'] === undefined) {
        return (null);
    }

    headerVal = headers['content-disposition'];
    try {
        parsedVal = contentDisposition.parse(headerVal);
        canonicalizedVal = contentDisposition(
            parsedVal.parameters.filename, parsedVal.type);
    } catch (err) {
        return new VError(err, 'invalid content-disposition header: "%s"',
            headerVal);
    }

    headers['content-disposition'] = canonicalizedVal;
    return (null);
}

///--- Exports

module.exports = {
    interleave: interleave,
    shallowCopy: shallowCopy,
    shuffle: shuffle,
    canonicalizeContentDisposition: canonicalizeContentDisposition
};
