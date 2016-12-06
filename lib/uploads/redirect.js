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
var libuuid = require('libuuid');
var path = require('path');
var restify = require('restify');



///--- API

/*
 * Gets the ID from the request URL, which is either of the form:
 *      /<account>/uploads/<id>
 * or
 *      /<account>/uploads/<id>/<partNum>
 */
function parseId(req, res, next) {

    if (req.params && req.params.partNum) {
        req.params.id = path.basename(path.dirname(req.url));
    } else {
        req.params.id = path.basename(req.url);
    }

    req.log.info('redirect started for id: ' + req.params.id);
    next();
}


/*
 * Redirects the request by looking up the upload path using the upload ID.
 */
function redirect(req, res, next) {
    var log = req.log;

    var prefix = req.params.id.charAt(0);
    var id = req.params.id;
    var url = '/' + req.params.account + '/uploads/' +  prefix + '/' + id;
    var key = '/' + req.key.split('/')[1] + '/uploads/' +  prefix + '/' + id;

    // Make sure this ID actually exists before sending a response.
    var opts = {
        key: key,
        requestId: req.getId()
    };
    req.moray.getMetadata(opts, function (err, record, wrap) {
        if (err) {
            next(err);
        } else {
            if (req.params.partNum) {
                url += '/' + req.params.partNum;
                key += '/' + req.params.partNum;
            }

            log.info('Redirecting \"' + req.url + '\"  to \"' + url + '\"');
            res.setHeader('Location', url);
            res.send(301);
            next();
        }
    });
}


///--- Exports

module.exports = {
    redirectHandler: function redirectHandler() {
        var chain = [
            parseId,
            redirect
        ];
        return (chain);
    }

};
