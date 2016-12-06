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
var path = require('path');
var restify = require('restify');


///--- Helpers


// Verifies the part num (as an integer) is within the range allowed.
function partNumInRange(pn) {
    assert.number(pn);
    return ((pn >= uploadsCommon.MIN_PART_NUM) &&
            (pn <= uploadsCommon.MAX_PART_NUM));
}

function partNumDefined(pn) {
    return (pn !== undefined && pn !== null);
}

///--- API

/*
 * Gets the ID from the request URL, which is either of the form:
 *      /<account>/uploads/<id>
 * or
 *      /<account>/uploads/<id>/<partNum>
 */
function parseId(req, res, next) {
    var log = req.log;

    log.debug({
        id: req.params.id,
        url: req.url,
        method: req.method
    }, 'redirect: requested');

    var pn = req.params.partNum;

    if (!partNumDefined(pn)) {
        // Path of the form /:account/uploads/:id.
        req.params.id = path.basename(req.url);
    } else if (uploadsCommon.PART_NUM_REGEX.test(pn)) {
        // Path of the form /:account/uploads/:id/:partNum
        req.params.id = path.basename(path.dirname(req.url));
    } else {
        next(new ResourceNotFoundError('part num ' + pn));
        return;
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

    var pn = req.params.partNum;
    if (partNumDefined(pn)) {
        url += '/' + pn;
    }

    log.debug({
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
