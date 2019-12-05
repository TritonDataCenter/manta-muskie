/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

require('./errors');


function rejectAllSnapLinks(req, res, next) {
    var log = req.log;

    next(new SnaplinksDisabledError('Manta v2 does not support SnapLinks.'));
}


///--- Exports

module.exports = {

    putLinkHandler: function () {
        var chain = [
            rejectAllSnapLinks
        ];
        return (chain);
    }

};
