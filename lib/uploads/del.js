/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var dir = require('../dir');
var obj = require('../obj');


/*
 * Typically, deletion of parts and uploaded directories from MPUs is not
 * allowed. However, we make this available to operator accounts provided
 * the query parameter "allowMpuDeletes=true" is provided in the URL.
 *
 * After verifying these checks, we call into the existing handlers for deleting
 * objects or directories to finish the DELETE.
 */


function checkOperator(req, res, next) {
    if (!req.caller.account.isOperator) {
        res.send(405);
        next(false);
    } else {
        next();
    }
}


function checkQueryParam(req, res, next) {
    if (req.query.allowMpuDeletes === 'true') {
        next();
    } else {
        res.send(422);
        next(false);
    }
}


///--- Exports

module.exports = {

    delUploadDirHandler: function delUploadDirHandler() {
        var chain = [
            checkOperator,
            checkQueryParam,
            dir.deleteDirectoryHandler()
        ];
        return (chain);
    },


    delPartHandler: function delPartHandler() {
        var chain = [
            checkOperator,
            checkQueryParam,
            obj.deleteObjectHandler()
        ];
        return (chain);
    }
};
