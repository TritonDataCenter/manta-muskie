/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var jsprim = require('jsprim');

var auth = require('../auth');
var obj = require('../obj');
var uploadsCommon = require('./common');
require('../errors');


///--- Globals

var hasKey = jsprim.hasKey;


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

    var partNum = req.params.partNum;
    var valid = uploadsCommon.PART_NUM_REGEX.test(partNum);

    if (!valid) {
        next(new MultipartUploadInvalidArgumentError(id,
            partNum + ' is not a valid part number'));
    } else {
        var state = req.upload.get(uploadsCommon.mdKeys.STATE);

        if (state !== uploadsCommon.MPU_S_CREATED) {
            next(new MultipartUploadStateError(id, 'already finalized'));
        } else {
            // Disallow changing the durability level of the part, as it is
            // determined when the MPU is created.
            if (req.headers && (hasKey(req.headers, 'durability-level') ||
                hasKey(req.headers, 'x-durability-level'))) {
                next(new MultipartUploadInvalidArgumentError(id,
                    'cannot change durability level for multipart uploads'));
                return;
            }

            log.debug({
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
        log.debug('zero-byte part');
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

module.exports = {
    uploadPartHandler: function uploadPartHandler() {
        var chain = [
            uploadsCommon.loadUpload,
            uploadsCommon.uploadContext,
            auth.authorizationHandler(),
            validate,
            setupPutState,

            // Piggyback on existing PUT code.
            obj.putPartHandler()
        ];

        return (chain);
    }
};
