/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var uploadsCommon = require('./common');


///--- API


function setupUpload(req, res, next) {
    var id = req.params.id;

    if (!id.match(uploadsCommon.ID_REGEX)) {
        next(new ResourceNotFoundError(req.url));
    } else {
        req.upload = new uploadsCommon.MultipartUpload(req, id);
        next();
    }
}


function getUpload(req, res, next) {
    req.upload.getUpload(function (err, upload) {
        if (err) {
            next(err);
        } else {
            res.send(200, upload);
            next();
        }
    });
}


///--- Exports

module.exports = {

    getHandler: function getHandler() {
        var chain = [
            setupUpload,
            getUpload
        ];
        return (chain);
    }
};
