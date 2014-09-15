/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var util = require('util');

var deepEqual = require('deep-equal');
var once = require('once');

var common = require('./common');
require('./errors');



///--- Globals

var sprintf = util.format;

var DIR_CONTENT_TYPE = 'application/x-json-stream; type=directory';



///--- Routes

//-- PUT Handlers --//

// If we can NOOP this request, we try to - in particular,
// marlin tends to slam the storage plane with mkdir requests,
// So we do a diff of the two objects (on the fields that
// matter); if they're the same, just tell the user "ok".
//
// Here's a sample directory request:
//
// {
//   dirname: '/7a36c494-fdb5-11e1-9204-1fcd0c6670ab/stor',
//   key: '/7a36c494-fdb5-11e1-9204-1fcd0c6670ab/stor/baz',
//   headers: {},
//   mtime: 1360278231120,
//   name: 'baz',
//   owner: '7a36c494-fdb5-11e1-9204-1fcd0c6670ab',
//   type: 'directory',
//   _etag: '59585943'
// }
function mkdir(req, res, next) {
    var log = req.log;
    var md = req.metadata;
    common.createMetadata(req, 'directory', function (err, opts) {
        if (err) {
            next(err);
            return;
        }
        log.debug({
            dir: req.key,
            metadata: opts,
            previousMetadata: md
        }, 'mkdir: entered');

        if (opts.dirname === md.dirname &&
            opts.key === md.key &&
            opts.owner === md.owner &&
            opts.type === md.type &&
            deepEqual(opts.headers, md.headers)) {
            log.debug({dir: req.key}, 'mkdir: noop');
            if (req.headers['origin']) {
                res.header('Access-Control-Allow-Origin',
                           req.headers['origin']);
            }
            res.send(204);
            next();
            return;
        }

        req.moray.putMetadata(opts, function (err2) {
            if (err2) {
                log.debug(err2, 'mkdir: failed');
                next(err2);
            } else {
                log.debug({dir: req.key}, 'mkdir: done');
                if (req.headers['origin']) {
                    res.header('Access-Control-Allow-Origin',
                               req.headers['origin']);
                }
                res.send(204);
                next();
            }
        });
    });
}



//-- GET Handlers --//

function getDirectoryCount(req, res, next) {
    if (req.metadata.type !== 'directory')
        return (next());

    var opts = {
        directory: req.key,
        requestId: req.id()
    };
    req.moray.getDirectoryCount(opts, function (err, count, obj) {
        if (err && err.name !== 'ObjectNotFoundError') {
            next(translateError(err, req));
        } else {
            req._dircount = count || 0;
            next();
        }
    });
}


function getDirectory(req, res, next) {
    if (req.metadata.type !== 'directory')
        return (next());

    var wroteHead = false;
    function writeHead(err) {
        if (!wroteHead) {
            if (err)
                return (true);

            common.addCustomHeaders(req, res);
            res.header('Content-Type', DIR_CONTENT_TYPE);
            res.header('Result-Set-Size', req._dircount || 0);
            res.writeHead(200);
            wroteHead = true;
            return (true);
        }
        return (false);
    }

    function done() {
        writeHead();
        res.end();
        next(false);
    }

    if (req.method !== 'HEAD') {
        var mreq = common.readdir(req.key, req);

        mreq.once('error', function (err) {
            mreq.removeAllListeners('end');
            mreq.removeAllListeners('entry');
            if (writeHead(err)) {
                next(err);
            } else {
                res.end();
                next(false);
            }
        });

        mreq.on('entry', function (entry) {
            writeHead();
            res.write(JSON.stringify(entry, null, 0) + '\n');
        });

        mreq.once('end', done);
    } else {
        done();
    }
}


function getRootDirectory(req, res, next) {
    if (req.method !== 'HEAD' && req.method !== 'GET')
        return (next(new RootDirectoryError(req)));

    var storagePaths = Object.keys(common.StoragePaths).sort();
    // Pagination is useless, but things like mfind rely on it.
    if (req.params.marker) {
        while (storagePaths.length > 0 &&
               storagePaths[0] <= req.params.marker) {
            storagePaths.shift();
        }
    }
    res.header('Content-Type', DIR_CONTENT_TYPE);
    res.header('Result-Set-Size', storagePaths.length);
    res.writeHead(200);

    if (req.method !== 'HEAD') {
        storagePaths.map(function (n) {
            // mtime: Happy b-day Manta!
            res.write(JSON.stringify({
                name: n,
                type: 'directory',
                mtime: '2013-05-22T17:39:43.714Z'
            }, null, 0) + '\n');
        });
    }
    res.end();
    return (next(false));
}


//-- DELETE Handlers --//

function ensureDirectoryEmpty(req, res, next) {
    if (req.metadata.type !== 'directory')
        return (next());

    var children = false;
    var mreq;

    // Just override the limit, so we don't ask moray to
    // do too much work
    req.params.limit = 1;
    mreq = common.readdir(req.key, req);

    mreq.once('error', next);

    mreq.once('entry', function (entry) {
        children = true;
    });

    mreq.once('end', function () {
        if (children) {
            next(new DirectoryNotEmptyError(req));
        } else {
            next();
        }
    });
}


function deleteDirectory(req, res, next) {
    if (req.metadata.type !== 'directory')
        return (next());

    var client = req.moray;
    var log = req.log;
    var opts = {
        key: req.key,
        _etag: req.isConditional() ? req.metadata._etag : undefined,
        requestId: req.getId()
    };

    log.debug('deleteDirectory: entered');
    client.delMetadata(opts, function (err) {
        if (err) {
            log.debug(err, 'deleteDirectory: error');
            next(err);
            return;
        }

        log.debug('deleteDirectory: done');
        res.send(204);
        next();
    });
}



///--- Exports

module.exports = {

    putDirectoryHandler: function putDirectoryHandler() {
        var chain = [
            // common.ensureNotRootHandler(),
            common.ensureParentHandler(),
            mkdir
        ];
        return (chain);
    },


    // read and delete assume getMetadata ran first

    getDirectoryHandler: function getDirectoryHandler() {
        var chain = [
            getDirectoryCount,
            getDirectory
        ];
        return (chain);
    },

    rootDirHandler: function topDirectoryHandler() {
        return (getRootDirectory);
    },

    deleteDirectoryHandler: function deleteDirectoryHandler() {
        var chain = [
            common.ensureNotRootHandler(),
            ensureDirectoryEmpty,
            deleteDirectory
        ];
        return (chain);
    }
};
