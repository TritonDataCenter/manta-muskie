/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var assert = require('assert-plus');
var fs = require('fs');
var path = require('path');
var util = require('util');

var restify = require('restify');
var VError = require('verror');

/*
 * This file exports errors into the global namespace, for both restify
 * and new errors. In order to add a new error to muskie, you should
 * add it to this file, and edit the javascript lint config file in this
 * repo to include the new error identifier (see jsl.node.conf for
 * examples). This will define the error for the linter so your code
 * is `make check` clean.
 */


///--- Globals

var sprintf = util.format;
var RestError = restify.RestError;


///--- Errors

function MuskieError(obj) {
    obj.constructorOpt = this.constructor;
    RestError.call(this, obj);
}
util.inherits(MuskieError, RestError);

function AccountDoesNotExistError(account) {
    MuskieError.call(this, {
        restCode: 'AccountDoesNotExist',
        statusCode: 403,
        message: sprintf('%s does not exist', account)
    });
}
util.inherits(AccountDoesNotExistError, MuskieError);

function AccountBlockedError(login) {
    MuskieError.call(this, {
        restCode: 'AccountBlocked',
        statusCode: 403,
        message: login + ' is not an active account'
    });
}
util.inherits(AccountBlockedError, MuskieError);


function AuthSchemeError(scheme) {
    MuskieError.call(this, {
        restCode: 'AuthorizationSchemeNotAllowed',
        statusCode: 403,
        message: (scheme || 'unknown scheme') +
            ' is not allowed (use \'signature\')'
    });
}
util.inherits(AuthSchemeError, MuskieError);


function AuthorizationError(login, _path, reason) {
    MuskieError.call(this, {
        restCode: 'AuthorizationFailed',
        statusCode: 403,
        message: login + ' is not allowed to access ' + _path,
        reason: reason
    });
}
util.inherits(AuthorizationError, MuskieError);


function AuthorizationRequiredError(reason) {
    MuskieError.call(this, {
        restCode: 'AuthorizationRequired',
        statusCode: 401,
        message: reason || 'Authorization is required'
    });
}
util.inherits(AuthorizationRequiredError, MuskieError);


function ChecksumError(expected, actual) {
    MuskieError.call(this, {
        restCode: 'ContentMD5Mismatch',
        statusCode: 400,
        message: sprintf('Content-MD5 expected %s, but was %s',
                         expected, actual)
    });
}
util.inherits(ChecksumError, MuskieError);


function ConcurrentRequestError(p) {
    MuskieError.call(this, {
        restCode: 'ConcurrentRequest',
        statusCode: 409,
        message: p + ' was being concurrently updated'
    });
}
util.inherits(ConcurrentRequestError, MuskieError);


function ContentLengthError() {
    MuskieError.call(this, {
        restCode: 'ContentLengthRequired',
        statusCode: 411,
        message: 'Content-Length must be >= 0'
    });
}
util.inherits(ContentLengthError, MuskieError);


function DirectoryDoesNotExistError(p) {
    MuskieError.call(this, {
        restCode: 'DirectoryDoesNotExist',
        statusCode: 404,
        message: sprintf('%s does not exist', path.dirname(p))
    });
}
util.inherits(DirectoryDoesNotExistError, MuskieError);


function DirectoryLimitError(p) {
    MuskieError.call(this, {
        restCode: 'DirectoryLimitExceeded',
        statusCode: 409,
        message: p + ' cannot exceed 1,000,000 entries'
    });
}
util.inherits(DirectoryLimitError, MuskieError);


function DirectoryNotEmptyError(req) {
    MuskieError.call(this, {
        restCode: 'DirectoryNotEmpty',
        statusCode: 400,
        message: sprintf('%s is not empty', req.path())
    });
}
util.inherits(DirectoryNotEmptyError, MuskieError);


function DirectoryOperationError(req) {
    MuskieError.call(this, {
        restCode: 'OperationNotAllowedOnDirectory',
        statusCode: 400,
        message: sprintf('%s is a directory: %s is not allowed',
                         req.path(), req.method)
    });
}
util.inherits(DirectoryOperationError, MuskieError);


function EntityExistsError(req) {
    MuskieError.call(this, {
        restCode: 'EntityAlreadyExists',
        statusCode: 409,
        message: sprintf('%s already exists', req.path())
    });
}
util.inherits(EntityExistsError, MuskieError);


function ExpectedUpgradeError(req) {
    MuskieError.call(this, {
        restCode: 'ExpectedUpgrade',
        statusCode: 400,
        message: sprintf('%s requires a Websocket Upgrade', req.path())
    });
}
util.inherits(ExpectedUpgradeError, MuskieError);


function InternalError(cause) {
    assert.ok(!cause || cause instanceof Error, 'cause');
    MuskieError.call(this, {
        restCode: 'InternalError',
        statusCode: 500,
        message: 'an unexpected error occurred',
        cause: cause
    });
}
util.inherits(InternalError, MuskieError);


function InvalidAlgorithmError(alg, valid) {
    var message = sprintf(
        '%s is not a supported signing algorithm. Supported algorithms are %s',
        alg, valid);

    MuskieError.call(this, {
        restCode: 'InvalidAlgorithm',
        statusCode: 401,
        message: message
    });
}
util.inherits(InvalidAlgorithmError, MuskieError);


function InvalidAuthTokenError(reason) {
    MuskieError.call(this, {
        restCode: 'InvalidAuthenticationToken',
        statusCode: 403,
        message: 'the authentication token you provided is ' +
            (reason || 'malformed')
    });
}
util.inherits(InvalidAuthTokenError, MuskieError);


function InvalidHttpAuthTokenError(reason) {
    MuskieError.call(this, {
        restCode: 'InvalidHttpAuthenticationToken',
        statusCode: 403,
        message: (reason || 'Invalid HTTP Auth Token')
    });
}
util.inherits(InvalidHttpAuthTokenError, MuskieError);


function InvalidDurabilityLevelError(min, max) {
    MuskieError.call(this, {
        restCode: 'InvalidDurabilityLevel',
        statusCode: 400,
        message: sprintf('durability-level must be between %d and %d',
                         min, max)
    });
}
util.inherits(InvalidDurabilityLevelError, MuskieError);


function InvalidKeyIdError() {
    MuskieError.call(this, {
        restCode: 'InvalidKeyId',
        statusCode: 403,
        message: 'the KeyId token you provided is invalid'
    });
}
util.inherits(InvalidKeyIdError, MuskieError);


function InvalidLimitError(l) {
    MuskieError.call(this, {
        restCode: 'InvalidArgumentError',
        statusCode: 400,
        message: 'limit=' + l + ' is invalid: must be between [1, 1024]'
    });
}
util.inherits(InvalidLimitError, MuskieError);


function InvalidLinkError(req) {
    MuskieError.call(this, {
        restCode: 'InvalidLink',
        statusCode: 400,
        message: sprintf('%s is an invalid link', req.headers.location)
    });
}
util.inherits(InvalidLinkError, MuskieError);


function InvalidUpdateError(k, extra) {
    MuskieError.call(this, {
        restCode: 'InvalidUpdate',
        statusCode: 400,
        message: 'overwrite of "' + k + '" forbidden' + (extra || '')
    });
}
util.inherits(InvalidUpdateError, MuskieError);


function InvalidParameterError(k, v) {
    MuskieError.call(this, {
        restCode: 'InvalidParameter',
        statusCode: 400,
        message: '"' + v + '" is invalid for "' + k + '"'
    });
}
util.inherits(InvalidParameterError, MuskieError);


function InvalidPathError(p) {
    MuskieError.call(this, {
        restCode: 'InvalidResource',
        statusCode: 400,
        message: '"' + p + '" is invalid'
    });
}
util.inherits(InvalidPathError, MuskieError);


function InvalidRoleError(r) {
    MuskieError.call(this, {
        restCode: 'InvalidRole',
        statusCode: 409,
        message: 'Role "' + r + '" is invalid.'
    });
}
util.inherits(InvalidRoleError, MuskieError);


function InvalidRoleTagError(r) {
    MuskieError.call(this, {
        restCode: 'InvalidRoleTag',
        statusCode: 409,
        message: 'Role tag "' + r + '" is invalid.'
    });
}
util.inherits(InvalidRoleTagError, MuskieError);


function InvalidSignatureError() {
    MuskieError.call(this, {
        restCode: 'InvalidSignature',
        statusCode: 403,
        message: 'The signature we calculated does not match the one ' +
            'you sent'
    });
}
util.inherits(InvalidSignatureError, MuskieError);


function KeyDoesNotExistError(account, key, user) {
    var message = user ?
            sprintf('/%s/%s/keys/%s does not exist', account, user, key) :
            sprintf('/%s/keys/%s does not exist', account, key);
    MuskieError.call(this, {
        restCode: 'KeyDoesNotExist',
        statusCode: 403,
        message: message
    });
}
util.inherits(KeyDoesNotExistError, MuskieError);


function LinkNotFoundError(req) {
    MuskieError.call(this, {
        restCode: 'SourceObjectNotFound',
        statusCode: 404,
        message: sprintf('%s was not found', req.headers.location)
    });
}
util.inherits(LinkNotFoundError, MuskieError);


function LinkNotObjectError(req) {
    MuskieError.call(this, {
        restCode: 'LinkNotObject',
        statusCode: 400,
        message: sprintf('%s is not an object', req.headers.location)
    });
}
util.inherits(LinkNotObjectError, MuskieError);


function LinkRequiredError() {
    MuskieError.call(this, {
        restCode: 'LocationRequired',
        statusCode: 400,
        message: 'An HTTP Location header must be specified'
    });
}
util.inherits(LinkRequiredError, MuskieError);


function SharksExhaustedError(res) {
    MuskieError.call(this, {
        restCode: 'InternalError',
        statusCode: 503,
        message: 'No storage nodes available for this request'
    });
    if (res)
        res.setHeader('Retry-After', 30);
}
util.inherits(SharksExhaustedError, MuskieError);


function MaxContentLengthError(len) {
    MuskieError.call(this, {
        restCode: 'InvalidMaxContentLength',
        statusCode: 400,
        message: len + ' is an invalid max-content-length value'
    });
}
util.inherits(MaxContentLengthError, MuskieError);


function MaxSizeExceededError(max) {
    MuskieError.call(this, {
        restCode: 'MaxContentLengthExceeded',
        statusCode: 413,
        message: 'request has exceeded ' + max + ' bytes'
    });
}
util.inherits(MaxSizeExceededError, MuskieError);


function MissingPermissionError(perm) {
    MuskieError.call(this, {
        restCode: 'MissingPermission',
        statusCode: 403,
        message: 'missing role allowing ' + perm
    });
}
util.inherits(MissingPermissionError, MuskieError);


function MultipartUploadCreateError(msg) {
    MuskieError.call(this, {
        restCode: 'MultipartUploadInvalidArgument',
        statusCode: 409,
        message: sprintf('cannot create upload: %s', msg)
    });
}
util.inherits(MultipartUploadCreateError, MuskieError);


function MultipartUploadStateError(id, msg) {
    MuskieError.call(this, {
        restCode: 'InvalidMultipartUploadState',
        statusCode: 409,
        message: sprintf('upload %s: %s', id, msg)
    });
}
util.inherits(MultipartUploadStateError, MuskieError);


function MultipartUploadInvalidArgumentError(id, msg) {
    MuskieError.call(this, {
        restCode: 'MultipartUploadInvalidArgument',
        statusCode: 409,
        message: sprintf('upload %s: %s', id, msg)
    });
}
util.inherits(MultipartUploadInvalidArgumentError, MuskieError);


function NotAcceptableError(req, type) {
    MuskieError.call(this, {
        restCode: 'NotAcceptable',
        statusCode: 406,
        message: sprintf('%s accepts %s', req.path(), type)
    });
}
util.inherits(NotAcceptableError, MuskieError);


function NoMatchingRoleTagError() {
    MuskieError.call(this, {
        restCode: 'NoMatchingRoleTag',
        statusCode: 403,
        message: 'None of your active roles are present on the resource.'
    });
}
util.inherits(NoMatchingRoleTagError, MuskieError);


function NotEnoughSpaceError(size, cause) {
    var message = sprintf('not enough free space for %d MB', size);

    MuskieError.call(this, {
        restCode: 'NotEnoughSpace',
        statusCode: 507,
        message: message,
        cause: cause
    });
}
util.inherits(NotEnoughSpaceError, MuskieError);


function NotImplementedError(message) {
    MuskieError.call(this, {
        restCode: 'NotImplemented',
        statusCode: 501,
        message: message
    });
}
util.inherits(NotImplementedError, MuskieError);


function ParentNotDirectoryError(req) {
    MuskieError.call(this, {
        restCode: 'ParentNotDirectory',
        statusCode: 400,
        message: sprintf('%s is not a directory',
                         path.dirname(req.path()))
    });
}
util.inherits(ParentNotDirectoryError, MuskieError);


function PreSignedRequestError(msg) {
    MuskieError.call(this, {
        restCode: 'InvalidQueryStringAuthentication',
        statusCode: 403,
        message: msg
    });
}
util.inherits(PreSignedRequestError, MuskieError);


// We expect an array of the offending parameters.
function QueryParameterForbiddenError(params) {
    MuskieError.call(this, {
        restCode: 'QueryParameterForbidden',
        statusCode: 403,
        message: sprintf(
            'Use of these query parameters is restricted to operators: %s',
            params.join(', '))
    });
}
util.inherits(QueryParameterForbiddenError, MuskieError);


function RangeNotSatisfiableError(req, err) {
    if (err && err._result && err._result.headers) {
        this.headers = {
            'Content-Range': err._result.headers['content-range']
        };
    }
    MuskieError.call(this, {
        restCode: 'RangeNotSatisfiable',
        statusCode: 416,
        message: sprintf('%s is an invalid range',
                         req.headers['range'])
    });
}
util.inherits(RangeNotSatisfiableError, MuskieError);


function RootDirectoryError(method, p) {
    MuskieError.call(this, {
        restCode: 'OperationNotAllowedOnRootDirectory',
        statusCode: 400,
        message: sprintf('%s is not allowed on %s',
                         method, p)
    });
}
util.inherits(RootDirectoryError, MuskieError);


function ResourceNotFoundError(p) {
    MuskieError.call(this, {
        restCode: 'ResourceNotFound',
        statusCode: 404,
        message: p + ' was not found'
    });
}
util.inherits(ResourceNotFoundError, MuskieError);


function ServiceUnavailableError(req, cause) {
    MuskieError.call(this, {
        restCode: 'ServiceUnavailable',
        statusCode: 503,
        message: 'manta is unable to serve this request',
        cause: cause
    });
}
util.inherits(ServiceUnavailableError, MuskieError);


function SnaplinksDisabledError(msg) {
    assert.string(msg, 'msg');
    MuskieError.call(this, {
        restCode: 'SnaplinksDisabledError',
        statusCode: 403,
        message: msg
    });
}
util.inherits(SnaplinksDisabledError, MuskieError);


function SSLRequiredError() {
    MuskieError.call(this, {
        restCode: 'SecureTransportRequired',
        statusCode: 403,
        message: 'Manta requires a secure transport (SSL/TLS)'
    });
}
util.inherits(SSLRequiredError, MuskieError);

function ThrottledError() {
    MuskieError.call(this, {
        restCode: 'ThrottledError',
        statusCode: 503,
        message: 'manta throttled this request'
    });
    this.name = this.constructor.name;
}
util.inherits(ThrottledError, MuskieError);

function UploadAbandonedError() {
    MuskieError.call(this, {
        restCode: 'UploadAbandoned',
        statusCode: 499,
        message: sprintf('request was aborted prematurely by the client')
    });
}
util.inherits(UploadAbandonedError, MuskieError);


function UploadTimeoutError() {
    MuskieError.call(this, {
        restCode: 'UploadTimeout',
        statusCode: 408,
        message: sprintf('request took too long to send data')
    });
}
util.inherits(UploadTimeoutError, MuskieError);


function UserDoesNotExistError(account, user) {
    var message;
    if (!account) {
        message = sprintf('%s does not exist', user);
    } else {
        message = sprintf('%s/%s does not exist', account, user);
    }
    MuskieError.call(this, {
        restCode: 'UserDoesNotExist',
        statusCode: 403,
        message: message
    });
}
util.inherits(UserDoesNotExistError, MuskieError);



///--- Translate API

function translateError(err, req) {
    if (err instanceof MuskieError || err instanceof restify.HttpError)
        return (err);

    var cause;

    // A NoDatabasePeersError with a context object that has a 'name' and a
    // 'message' field sent from Moray indicates a transient availability error
    // due to request overload. We report these types of errors as 503s to
    // indicate to the client that it can ameliorate the situation with
    // sufficient back-off. A NoDatabasePeersError without a context object
    // of this form still indicates an internal error (500) that should be
    // investigated as a bug or other more serious problem in Manta.
    cause = VError.findCauseByName(err, 'NoDatabasePeersError');
    if (cause !== null && cause.context && cause.context.name &&
        cause.context.message && cause.context.name === 'OverloadedError') {
        return (new ServiceUnavailableError(req, new VError({
            'name': cause.context.name
        }, '%s', cause.context.message)));
    }

    cause = VError.findCauseByName(err, 'ThrottledError');
    if (cause !== null) {
        return (cause);
    }

    cause = VError.findCauseByName(err, 'ObjectNotFoundError');
    if (cause !== null) {
        return (new restify.ResourceNotFoundError(cause,
            '%s does not exist', req.path()));
    }

    cause = VError.findCauseByName(err, 'PreconditionFailedError');
    if (cause !== null) {
        return (cause);
    }

    cause = VError.findCauseByName(err, 'RangeNotSatisfiableError');
    if (cause !== null) {
        return (new RangeNotSatisfiableError(req, cause));
    }

    cause = VError.findCauseByName(err, 'EtagConflictError');
    if (cause === null) {
        cause = VError.findCauseByName(err, 'UniqueAttributeError');
    }
    if (cause !== null) {
        return (new ConcurrentRequestError(req.path()));
    }

    return (new InternalError(err));
}


///--- Exports

// Auto export all Errors defined in this file
fs.readFileSync(__filename, 'utf8').split('\n').forEach(function (l) {
    /* JSSTYLED */
    var match = /^function\s+(\w+)\(.*/.exec(l);
    if (match !== null && Array.isArray(match) && match.length > 1) {
        if (/\w+Error$/.test(match[1])) {
            module.exports[match[1]] = eval(match[1]);
        }
    }
});

Object.keys(module.exports).forEach(function (k) {
    global[k] = module.exports[k];
});
