/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var restify = require('restify');

var auth = require('../auth');
var uploadsCommon = require('./common');
require('../errors');


///--- API



/*
 * Ensures that the upload is in the correct state: either created or aborted,
 * and updates the upload record's metadata if needed to reflect this state.
 */
function finalizingState(req, res, next) {
    var log = req.log;
    var upload = req.upload;
    var id = upload.id;

    var state = req.upload.get(uploadsCommon.mdKeys.STATE);
    var type = req.upload.get(uploadsCommon.mdKeys.TYPE);

    log.debug({
        uploadId: id,
        uploadState: state,
        finalizingType: type ? type : 'N/A'
    }, 'abort: requested');

    if (state === uploadsCommon.MPU_S_CREATED) {
        assert.ok(!type);
        upload.finalizeUploadRecord(uploadsCommon.MPU_FT_ABORT, null,
        function (err2) {
            if (err2) {
                next(err2);
            } else {
                next();
            }
        });
    } else if ((state === uploadsCommon.MPU_S_FINALIZING) &&
        (type === uploadsCommon.MPU_FT_ABORT)) {

        log.debug('abort already in progress for upload ' + id);
        next();

    } else if ((state === uploadsCommon.MPU_S_FINALIZING) &&
        (type === uploadsCommon.MPU_FT_COMMIT)) {

        next(new MultipartUploadStateError(id, 'already committed'));

    } else {
        assert.fail('Invalid state/type combination for upload: '
            + state + '/' + type);
     }
}


function abort(req, res, next) {
    var log = req.log;

    req.upload.abortUpload(function (err) {
        if (err) {
            next(err);
        } else {
            log.debug({
                uploadId: req.upload.id
            }, 'abort: completed');

            res.setHeader('Content-Length', '0');
            res.send(204);
            next();
        }
    });
}


///--- Exports

module.exports = {
    abortHandler: function abortHandler() {
        var chain = [
            uploadsCommon.loadUploadFromUrl,
            uploadsCommon.uploadContext,
            auth.authorizationHandler(),
            finalizingState,
            abort
        ];
        return (chain);
    }
};
