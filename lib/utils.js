/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var assert = require('assert-plus');
var contentDisposition = require('content-disposition');

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
 * validates, and sets a content-disposition header, if present
 *
 * If a content-dispositon key is present in the passed headers
 * object, it is validated and canonicalised. The canonical, valid
 * value is saved against the headers 'content-disposition' key and
 * callback called with headers. If the value is invalid, an error is
 * passed to the callback, the error.msg will be the invalid
 * content-disposition string.
 */
function validateContentDisposition(headers, cb) {
    assert.object(headers, 'headers');
    assert.func(cb, 'callback');

    if (headers['content-disposition'] !== undefined) {
        var cd = headers['content-disposition'];

        try {
            // use a round trip of parse and write to set a valid
            // value that can be used when reading the object.
            var cdp = contentDisposition.parse(cd);
            var cdStr = contentDisposition(cdp.parameters.filename,
                                           cdp.type);
            headers['content-disposition'] = cdStr;
            cb(null, headers);
        } catch (err) {
            err.msg = cd;
            cb(err, headers);
        }
    } else {
        cb(null, headers);
    }
}

///--- Exports

module.exports = {
    interleave: interleave,
    shallowCopy: shallowCopy,
    shuffle: shuffle,
    validateContentDisposition: validateContentDisposition
};
