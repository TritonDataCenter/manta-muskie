/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

function reExport(obj) {
    Object.keys(obj || {}).forEach(function (k) {
        module.exports[k] = obj[k];
    });
}

module.exports = {};
reExport(require('./create'));
reExport(require('./delete'));
reExport(require('./head'));
reExport(require('./list'));
reExport(require('./objects/create'));
reExport(require('./objects/delete'));
reExport(require('./objects/get'));
reExport(require('./objects/head'));
reExport(require('./objects/list'));
