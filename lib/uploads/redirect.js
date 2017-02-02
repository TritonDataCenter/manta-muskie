/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var uploadsCommon = require('./common');

var assert = require('assert-plus');
var libuuid = require('libuuid');
var path = require('path');
var restify = require('restify');



///--- API

/*
 * Gets the ID from the request URL, which is either of the form:
 *      /<account>/uploads/<id>
 * or
 *      /<account>/uploads/<id>/<partNum>
 */
function parseId(req, res, next) {
    var log = req.log;

    log.info({
        id: req.params.id,
        url: req.url,
        method: req.method
    }, 'redirect: requested');

    if (req.params && req.params.partNum) {
        req.params.id = path.basename(path.dirname(req.url));
    } else {
        req.params.id = path.basename(req.url);
    }

    next();
}


/*
 * Redirects the request by looking up the upload path using the upload ID.
 */
function redirect(req, res, next) {
    var log = req.log;

    // We want to get the upload path from the loaded metadata of the upload,
    // as opposed to what's on the object itself.
    var url = req.upload.get(uploadsCommon.mdKeys.UPLOAD_PATH);
    if (req.params.partNum) {
        url += '/' + req.params.partNum;
    }

    log.info({
        id: req.params.id,
        url: req.url,
        method: req.method,
        redirectLocation: url
    }, 'redirect: completed');

    res.setHeader('Location', url);
    res.send(301);
    next();
}


///--- Exports

module.exports = {
    redirectHandler: function redirectHandler() {
        var chain = [
            parseId,
            uploadsCommon.loadUpload,
            redirect
        ];
        return (chain);
    }

};
