/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

///--- Helpers

function reExport(obj) {
    Object.keys(obj || {}).forEach(function (k) {
        module.exports[k] = obj[k];
    });
}



///--- Exports

module.exports = {};
reExport(require('./abort'));
reExport(require('./create'));
reExport(require('./commit'));
reExport(require('./del'));
reExport(require('./get'));
reExport(require('./redirect'));
reExport(require('./upload'));
