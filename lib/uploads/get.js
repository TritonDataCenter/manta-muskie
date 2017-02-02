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


function getUpload(req, res, next) {
    var log = req.log;

    log.info({
        id: req.upload.id
    }, 'get-mpu: requested');

    req.upload.getUpload(function (err, upload) {
        if (err) {
            next(err);
        } else {
            log.info({
                id: req.upload.id,
                upload: upload
            }, 'get-mpu:completed');

            res.send(200, upload);
            next();
        }
    });
}


///--- Exports

module.exports = {

    getHandler: function getHandler() {
        var chain = [
            uploadsCommon.loadUpload,
            getUpload
        ];
        return (chain);
    }
};
