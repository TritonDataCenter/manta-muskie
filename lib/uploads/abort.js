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

var uploadsCommon = require('./common');
require('../errors');


///--- API



/*
 * Ensures that the upload is in the correct state: either created or aborted,
 * and updates the upload record's metadata if needed to reflect this state.
 *
 */
function finalizingState(req, res, next) {
    var log = req.log;
    var upload = req.upload;

    req.upload.uploadState(function (err, state, type) {
        if (err) {
            next(err);
        } else {
            var states = uploadsCommon.uploadStates;
            var types = uploadsCommon.uploadTypes;

            if (state === states.CREATED) {
                assert.ok(!type);
                upload.finalizeUploadRecord(types.ABORT, null, function (err2) {
                        if (err2) {
                            next(err2);
                        } else {
                            next();
                        }
                });
            } else if ((state === states.FINALIZING) &&
                (type === types.ABORT)) {

                log.debug('abort already in progress for upload ' + upload.id);
                next();

            } else if ((state === states.FINALIZING) &&
                (type === types.COMMIT)) {

                log.debug('commit already in progress for upload ' + upload.id);
                next(new MultipartUploadFinalizeConflictError(upload.id,
                    types.COMMIT));

            } else {
                assert.fail('Invalid state/type combination for upload: '
                    + state + '/' + type);
            }
        }
    });
}


function abort(req, res, next) {
    req.upload.abortUpload(function (err) {
        if (err) {
            next(err);
        } else {
            req.log.info('upload ' + req.upload.id + ' aborted');
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
            uploadsCommon.setupUpload,
            finalizingState,
            abort
        ];
        return (chain);
    }
};
