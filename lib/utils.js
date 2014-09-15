/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

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


function shuffle(array) {
    var current;
    var tmp;
    var top = array.length;

    if (top) {
        while (--top) {
            current = Math.floor(Math.random() * (top + 1));
            tmp = array[current];
            array[current] = array[top];
            array[top] = tmp;
        }
    }

    return (array);
}



///--- Exports

module.exports = {
    interleave: interleave,
    shallowCopy: shallowCopy,
    shuffle: shuffle
};