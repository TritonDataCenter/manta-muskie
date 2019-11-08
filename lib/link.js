/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent Inc.
 */

require('./errors');


function snapLinksDisabled(req, res, next) {
    var log = req.log;

    log.warn({
        link: req.link
    }, 'Rejecting attempt to create SnapLink');

    next(new SnaplinksDisabledError('Manta v2 does not support SnapLinks.'));
}


///--- Exports

module.exports = {

    putLinkHandler: function () {
        var chain = [
            snapLinksDisabled
        ];
        return (chain);
    }

};
