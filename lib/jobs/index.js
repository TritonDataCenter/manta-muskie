/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

///--- Helpers

function reExport(obj) {
    Object.keys(obj || {}).forEach(function (k) {
        module.exports[k] = obj[k];
    });
}



///--- Exports

module.exports = {};
reExport(require('./create'));
reExport(require('./list'));
reExport(require('./get'));
reExport(require('./input'));
reExport(require('./output'));
reExport(require('./error'));
reExport(require('./fail'));
reExport(require('./post'));
