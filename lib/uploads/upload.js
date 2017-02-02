/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var libuuid = require('libuuid');
var path = require('path');
var restify = require('restify');
var vasync = require('vasync');

var auth = require('../auth');
var common = require('../common');
var obj = require('../obj');
var uploadsCommon = require('./common');
require('../errors');

//TODO: enforce max individual part size based on object size

///--- API

/*
 * Does some basic validation on the part before proceeding to the normal PUT
 * path, including:
 *   - ensuring the partNum is valid
 *   - ensuring the upload hasn't been finalized yet
 *   - ensuring client isn't trying to change the number of copies of the object
 */
function validate(req, res, next) {
    var log = req.log;
    var id = req.upload.id;
    log.info('validate');

    var regex = /^([0-9]|[1-9][0-9]{0,3})$/;
    var partNum = req.params.partNum;
    var valid = regex.test(partNum);

    if (!valid) {
        next(new MultipartUploadPartNumError(req.upload.id, partNum));
    } else {
        var state = req.upload.get(uploadsCommon.mdKeys.STATE);

        if (state !== uploadsCommon.uploadStates.CREATED) {
            next(new MultipartUploadFinalizeConflictError(id,
                'upload part for'));
        } else {
                log.info({
                    uploadId: id,
                    partNum: partNum,
                    headers: req.headers
                }, 'upload-part: requested');

                next();
        }
    }
}


/*
 * The PUT handling code relies on some state being set up on the request
 * object that is done by handlers not used for uploading parts.
 *
 * This handler ensures that the state needed for the PUT handling code
 * is available so that the PUT handling code we do use for uploading
 * parts works seamlessly.
 */
function setupPutState(req, res, next) {
    var log = req.log;
    var upload = req.upload;

    // Ensure zero-byte objects aren't streamed to mako.
    if (req.upload.uploadSize() === 0) {
        log.info('zero-byte part');
        req._zero = true;
    }

    // Ensure that the PUT handling code can find the correct sharks to use.
    req._sharks = [upload.get(uploadsCommon.mdKeys.SHARKS)];

    // Fake a durability-level header that matches the header
    // specified on upload creation.
    req.headers['durability-level'] = req.upload.numSharks();

    next();
}


///--- Exports

//  PUT handlers not included in this chain, in order:
//      - conditionalRequest()
//      - ensureNotRootHandler()
//      [parseArguments]
//      - ensureNotDirectoryHandler()
//      - ensureParentHandler()
//      - enforceDirectoryCount
//      [other PUT handlers]
//
//      TODO: I think I need: conditionalRequest, enforceDirectoryCount
module.exports = {
    uploadPartHandler: function uploadPartHandler() {
        var chain = [
            uploadsCommon.loadUpload,
            uploadsCommon.uploadContext,
            auth.authorizationHandler(),
            validate,
            setupPutState,

            // Piggybacking on existing PUT code.
            //restify.conditionalRequest,//TODO: this makes request hang
            obj.parseArguments,
            obj.startSharkStreams,
            obj.sharkStreams,
            obj.saveMetadata
        ];

        return (chain);
    }
};
