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

/*
 * lib/uploads/redirect.js: Multipart Upload Redirect Endpoint
 *
 * SUMMARY:
 *
 * The multipart upload redirect API provides a mechanism to map multipart
 * upload IDs to their fully qualified upload directory.
 *
 * MOTIVATION:
 *
 * As a user of the multipart upload API, it is often preferable to operate
 * with the upload ID as the primary handle for a multipart upload -- for
 * example, short-lived client sessions (such as command line interfaces) might
 * only take as input the upload ID. The server-side API, however, requires
 * fully qualified paths to the upload directory for use. Thus, it is desirable
 * to have a server-side mechanism to resolve upload IDs to their upload paths.
 *
 * CHALLENGES:
 *
 * In order to resolve an upload path from an upload ID, the server must have
 * knowledge of the directory structure under the top-level "uploads"
 * directory *at the time the upload was created*. This directory structure
 * is tunable by operators based on the prefix length used for the "uploads"
 * sub-directories (see the comment in `lib/uploads/common.js` for details on
 * the motivation for the prefix length).
 *
 * It is not sufficient to map upload IDs to upload paths by assuming the prefix
 * the upload was created with is the same prefix length the server is using
 * today. In order to deterministically map an id to its path, then, the server
 * needs another way of discovering the prefix.
 *
 * Because the server cannot control when the tunable is applied, any scheme for
 * this mapping should also be compatible with all multipart uploads before the
 * tunable existed. Prior to the tunable, all upload IDs were randomly generated
 * uuids.
 *
 * It is tempting to consider an alternative method, in which upload IDs
 * continue to be randomly generated, and the server searches every possible
 * prefix. Such a strategy creates a variable amount of work per redirect
 * request on the server side, and thus should be avoided.
 *
 * SOLUTION:
 *
 * A new scheme for upload IDs was proposed.
 *
 * The upload ID instead encodes the prefix length in the ID itself:
 * specifically, as a hex value in the last character of the id. To map an
 * upload id to its upload path, then, the server extracts the prefix length
 * from the id and generates the prefix appropriately. This scheme allows the
 * server to know where the upload path is expected to be, and definitively say
 * if the resource exists (as opposed to: it doesn't exist assuming a given
 * prefix length), for all ids under the new scheme.
 *
 * Accommodating historical ids is also much better: first, the server attempts
 * to resolve the upload path assuming the ID was generated from the modern
 * scheme. If that path doesn't exist, then it tries the formerly hard-coded
 * prefix length value of 1. If the upload path doesn't exist, then it doesn't
 * exist at all.
 *
 * In the common case, the server makes 1 metadata request, and in the worst
 * case, it makes two requests. This is far more optimal than searching the
 * entire prefix directory space, and provides clients a convenience to look up
 * the fully qualified upload path if they only have an upload ID.
 *
 */

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


/*
 * Redirects the request by looking up the upload path using the upload ID.
 */
function redirect(req, res) {
    assert.object(req, 'req');
    assert.object(req.upload, 'req.upload');
    assert.string(req.upload.uploadPath, 'req.upload.uploadPath');

    var log = req.log;

    var url = req.upload.uploadPath;

    var pn = req.params.partNum;
    if (partNumDefined(pn)) {
        url += '/' + pn;
    }

    log.debug({
        id: req.params.id,
        url: req.url,
        method: req.method,
        redirectLocation: url
    }, 'redirect: sending');

    res.setHeader('Location', url);
    res.send(301);
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

    // Disallow subusers.
    if (req.caller.user) {
        next(new AuthorizationError(req.caller.user.login, req.url));
        return;
    }

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
 * Check if there is an upload record for the upload ID under the modern ID
 * generation scheme.
 */
function checkUploadDir(req, res, next) {
    var id = req.params.id;
    assert.uuid(id, 'id');

    var opts = {
        id: id,
        login: req.owner.account.login
    };

    /*
     * If the prefix is higher than the max length, than it can't be one from
     * the modern scheme.
     */
    if (uploadsCommon.idToPrefixLen(id) > uploadsCommon.MAX_PREFIX_LEN) {
        next();
        return;
    }

    var uploadPath = uploadsCommon.generateUploadPath(opts);

    req.upload = new uploadsCommon.MultipartUpload(id, uploadPath, req);
    req.upload.uploadRecordExists(function (err, exists) {
        if (err) {
            next(err);
        } else {
            if (exists) {
                redirect(req, res);
                next(false);
            } else {
                next();
            }
        }
    });
}

/*
 * Check if there is an upload record for the upload ID under the legacy ID
 * generation scheme.
 *
 * If it doesn't exist, then the upload doesn't exist, and we can safely return
 * a 404.
 */
function checkUploadDirLegacy(req, res, next) {
    var id = req.params.id;
    assert.uuid(id, 'id');

    var opts = {
        id: id,
        login: req.owner.account.login,
        legacy: true
    };

    var uploadPath = uploadsCommon.generateUploadPath(opts);

    req.upload = new uploadsCommon.MultipartUpload(id, uploadPath, req);
    req.upload.uploadRecordExists(function (err, exists) {
        if (err) {
            next(err);
        } else {
            if (exists) {
                redirect(req, res);
                next();
            } else {
                next(new ResourceNotFoundError(req.path()));
            }
        }
    });
}


///--- Exports

module.exports = {
    redirectHandler: function redirectHandler() {
        var chain = [
            parseId,
            checkUploadDir,
            checkUploadDirLegacy
        ];
        return (chain);
    }

};
