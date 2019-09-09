// Copyright 2012 Mark Cavage, Inc.  All rights reserved.

/*
 * Copyright 2019 Joyent, Inc.
 */

var assert = require('assert-plus');
var restify = require('restify');

///--- Globals

var BadRequestError = restify.BadRequestError;
var PreconditionFailedError = restify.PreconditionFailedError;

var IF_MATCH_FAIL = 'if-match \'%s\' didn\'t match etag \'%s\'';
var IF_NO_MATCH_FAIL = 'if-none-match \'%s\' matched etag \'%s\'';
var IF_MOD_FAIL = 'object was modified at \'%s\'; if-modified-since \'%s\'';
var IF_UNMOD_FAIL = 'object was modified at \'%s\'; if-unmodified-since \'%s\'';


///--- API
// Reference RFC2616 section 14 for an explanation of what this all does.

function checkIfMatch(req, res, next) {
    assert.bool(req.resource_exists, 'resource_exists');

    var clientETags;
    var cur;
    var etag = res.etag || res.getHeader('etag') || '';
    var ifMatch;
    var matched = false;

    if ((ifMatch = req.headers['if-match'])) {
        /* JSSTYLED */
        clientETags = ifMatch.split(/\s*,\s*/);

        for (var i = 0; i < clientETags.length; i++) {
            cur = clientETags[i];
            // only strong comparison
            /* JSSTYLED */
            cur = cur.replace(/^W\//, '');
            /* JSSTYLED */
            cur = cur.replace(/^"(\w*)"$/, '$1');

            if (cur === '*') {
                matched = req.resource_exists;
                break;
            } else if (req.resource_exists && cur === etag) {
                matched = true;
                break;
            }
        }

        if (!matched) {
            var err = new PreconditionFailedError(IF_MATCH_FAIL,
                ifMatch,
                etag);
            return (next(err));
        }
    }

    return (next());
}


function checkIfNoneMatch(req, res, next) {
    assert.bool(req.resource_exists, 'resource_exists');

    var clientETags;
    var cur;
    var etag = res.etag || res.getHeader('etag') || '';
    var ifNoneMatch;
    var matched = false;

    if ((ifNoneMatch = req.headers['if-none-match'])) {
        /* JSSTYLED */
        clientETags = ifNoneMatch.split(/\s*,\s*/);

        for (var i = 0; i < clientETags.length; i++) {
            cur = clientETags[i];
            // ignore weak validation
            /* JSSTYLED */
            cur = cur.replace(/^W\//, '');
            /* JSSTYLED */
            cur = cur.replace(/^"(\w*)"$/, '$1');

            if (cur === '*') {
                matched = req.resource_exists;
            } else if (req.resource_exists && cur === etag) {
                matched = true;
                break;
            }
        }

        if (matched) {
            // If request method is not GET or HEAD then return 412
            if (req.method !== 'GET' && req.method !== 'HEAD') {
                var err = new PreconditionFailedError(IF_NO_MATCH_FAIL,
                                                      ifNoneMatch,
                                                      etag);
                next(err);
            } else {
                // For GET or HEAD return 304 Not Modified
                res.send(304);
                next(false);
            }
            return;
        }
    }

    next();
}


function checkIfModified(req, res, next) {
    var code;
    var err;
    var ctime = req.header('if-modified-since');
    var mtime = res.mtime || res.header('Last-Modified') || '';

    if (!mtime || !ctime) {
        next();
        return;
    }

    try {
        //
        // TODO handle Range header modifications
        //
        // Note: this is not technically correct as per 2616 -
        // 2616 only specifies semantics for GET requests, not
        // any other method - but using if-modified-since with a
        // PUT or DELETE seems like returning 412 is sane
        //
        if (Date.parse(mtime) <= Date.parse(ctime)) {
            switch (req.method) {
                case 'GET':
                case 'HEAD':
                    code = 304;
                    break;

                default:
                    err = new PreconditionFailedError(IF_MOD_FAIL,
                        mtime,
                        ctime);
                    break;
            }
        }
    } catch (e) {
        next(new BadRequestError(e.message));
        return;
    }

    if (code !== undefined) {
        res.send(code);
        next(false);
        return;
    }

    next(err);
}


function checkIfUnmodified(req, res, next) {
    var err;
    var ctime = req.headers['if-unmodified-since'];
    var mtime = res.mtime || res.header('Last-Modified') || '';

    if (!mtime || !ctime) {
        next();
        return;
    }

    try {
        if (Date.parse(mtime) > Date.parse(ctime)) {
            err = new PreconditionFailedError(IF_UNMOD_FAIL,
                mtime,
                ctime);
        }
    } catch (e) {
        next(new BadRequestError(e.message));
        return;
    }

    next(err);
}


///--- Exports

/**
 * Returns a set of plugins that will compare an already set ETag header with
 * the client's If-Match and If-None-Match header, and an already set
 * Last-Modified header with the client's If-Modified-Since and
 * If-Unmodified-Since header.
 */

module.exports = {
    conditionalRequest: function _conditionalRequest() {
        var chain = [
            checkIfMatch,
            checkIfUnmodified,
            checkIfNoneMatch,
            checkIfModified
        ];
        return (chain);
    },

    matchConditionalRequest: function _matchConditionalRequest() {
        var chain = [
            checkIfMatch,
            checkIfNoneMatch
        ];
        return (chain);
    },

    modifiedConditionalRequest: function _modifiedConditionalRequest() {
        var chain = [
            checkIfUnmodified,
            checkIfModified

        ];
        return (chain);
    }
};
