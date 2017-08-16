---
title: Joyent Manta Service REST API
markdown2extras: wiki-tables, code-friendly
apisections: Directories, Objects, Links, Compute, Jobs, SnapLinks
---
<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2015, Joyent, Inc.
-->

# REST API

This is the API reference documentation for the Joyent Manta Storage
Service, which enables you to store data in the cloud and process that data
using the built-in compute facility.

This document covers only the HTTP interface and all examples are given in curl.

Before you do the examples in this section, it's important to go through the examples using the CLI and setting up your environment.

* [Getting Started](index.html)

There is also detailed reference materials:

* [Object Storage Reference](storage-reference.html)
* [Compute Jobs Reference](jobs-reference.html)

## Conventions

Any content formatted like this:

    $ curl -is https://us-east.manta.joyent.com

is a command-line example that you can run from a shell. All other examples and
information are formatted like this:

    GET /my/stor/foo HTTP/1.1


# Authentication

There are a few access methodologies. The predominant means of
authenticating requests to the service is to use the
[HTTP Signature](http://tools.ietf.org/html/draft-cavage-http-signatures-00)
over TLS.

In most situations, you will only need to sign the lowercase `date: ` and value
of the HTTP `Date` header using your SSH private key; doing this allows you to
create interactive shell functions (see below).  All requests require an HTTP
Authorization header where the scheme is `Signature`.

Full details are available in the `http signatures` specification, but a simple
form is:

    Authorization: Signature keyId="/:login/keys/:fp",algorithm="rsa-sha256",signature="$base64_signature"

The `keyId` for the service is always
`/$your_joyent_login/keys/$ssh_fingerprint`, and the supported algorithms are:
`rsa-sha1`, `rsa-sha256` and `dsa-sha`. The ssh key fingerprint must be a MD5
fingerprint (ex. `a1:b2:c3:d4:e5:f6:a7:b8:c9:d0:e1:f2:a3:b4:c5:d6`)

To make a request for an RBAC subuser, change the `keyId` for the signature to
`/$your_joyent_login/$subuser_login/keys/$ssh_fingerprint`. To make a request
using a RBAC role, include the HTTP header `Role`.

## Signed URLS

It can be desirable to share "expiring links" with someone via a
simple link, suitable for a web browser or email client.  In these cases, the service
supports a "signed URL" scheme, as an alternative to making programmatic REST
requests (there is no Authorization header).  Signed URLs are the only exception
to the HTTPS-only requirement of the service; you are allowed to perform GET requests
only over HTTP via this mechanism to avoid "mixed content" errors in some web
browsers.  The query string must at minimum must contain the same identifying
parameters that would be in the Authorization header (user, keyId, algorithm).
Additionally it *must* contain an `expires` parameter, which is a time the URL
is valid until, specified as the number of seconds from the Unix epoch.

To construct a signed URL, you:

+ Start with the HTTP Request Method + a new line
+ Add the value of the HTTP Host header + a new line
+ Add the HTTP Request URI, minus any query parameters + a new line
+ Add a line of the HTTP query parameters, sorted lexicographically, and where
  the keys and values are encoded as per
  [RFC3986](http://www.ietf.org/rfc/rfc3986.txt).

Formalized:

    HTTP REQUEST Method + '\n'
    Host Header + '\n'
    HTTP REQUEST URI + '\n
    key=val&key=val... (query parameters, url, sorted lexicographically)

For example, suppose you wanted to share a link to
`https://us-east.manta.joyent.com/$MANTA_USER/stor/image.png`.  The following
would be the signing string (newlines inserted for readability):

    GET\n
    us-east.manta.joyentcloud.com\n
    /$MANTA_USER/stor/image.png
    algorithm=RSA-SHA256&expires=1354201912&keyId=%2F$MANTA_USER%2Fkeys%2F8e%3A36%3A43%3Aac%3Ad0%3A61%3A60%3A18%3A20%3Af5%3Ab7%3Aec%3A3a%3Ad8%3A79%3A2d

You would sign that entire string with your private key, URL encode the
signature and then append it to the URL query string:

    https://us-east.manta.joyent.com/$MANTA_USER/stor/image.png?algorithm=RSA-SHA256&expires=1354201912&keyId=%2F$MANTA_USER%2Fkeys%2F8e%3A36%3A43%3Aac%3Ad0%3A61%3A60%3A18%3A20%3Af5%3Ab7%3Aec%3A3a%3Ad8%3A79%3A2d&signature=RR5s5%2Fa0xpwvukU2tn3LAe2QRHGRJVdbWu%2FQZk%2BYgnVmuWI59n8EG0G6KN5INp30r7xC0EOSMvgmyfrLFQTG1482fNsjedwfFXVZq0%2BeUV6dI36pe69FxMuRKh4ILy47l6wCqD4qvFsFwmeqmzfkn03MmU15JsJt2yqTtnz%2FGkToZCpaHugW5YferGNeAY%2FrTwLTrB%2BrsKovY35rK9eokPbJTDlNx97JX5%2F7ol3cgtRbstLuROfpCycJ5OxC3NAeXeUD7weGQxAY6ypoEq5HFiZoA3gT4lDdYyO7LKKPkE8dSqcqVqgdtflpf%2FYibKwGg5Vm%2F9Ze%2Fwq%2Bsb1RgSAJsA%3D%3D

## Interacting with the Joyent Manta Storage Service from the shell (bash)

Most things in the service are easy to interact with via [cURL](http://curl.haxx.se/),
but note that all requests to the service must be authenticated.  You can string
together [OpenSSL](http://www.openssl.org/) and [cURL](http://curl.haxx.se/)
with a bash function.

Copy all of below:

      function manta {
		    local alg=rsa-sha256
		    local keyId=/$MANTA_USER/keys/$MANTA_KEY_ID
		    local now=$(date -u "+%a, %d %h %Y %H:%M:%S GMT")
		    local sig=$(echo "date:" $now | \
		                tr -d '\n' | \
		                openssl dgst -sha256 -sign $HOME/.ssh/id_rsa | \
		                openssl enc -e -a | tr -d '\n')

		    curl -sS $MANTA_URL"$@" -H "date: $now"  \
		        -H "Authorization: Signature keyId=\"$keyId\",algorithm=\"$alg\",signature=\"$sig\""
		}

Paste into `~/.bash_profile` or `~/.bashrc` and restart your terminal to pick up the changes.

    pbpaste > ~/.bash_profile

And edit the file, replacing `$JOYENT_CLOUD_USER_NAME` with your actual cloud username.

This all is setup correctly you will be able to:

    $ manta /$MANTA_USER/stor
    $ manta /$MANTA_USER/stor/moved -X PUT -H "content-type: application/json; type=file"
    $ manta /$MANTA_USER/stor/foo -X PUT -H "content-type: application/json; type=directory"
    $ manta /$MANTA_USER/stor -X GET
    {"name":"foo","type":"directory","mtime":"2013-06-16T05:42:56.515Z"}
      {"name":"moved","etag":"bfaa3227-3abb-4ed6-915a-a2179f623172","size":0,"type":"object","mtime":"2013-06-16T05:42:45.460Z"}

All sample "curl" requests in the rest of this document use the
function above.  Throughout the rest of the document, the value of the
Authorization header is simply represented as `$Authorization`.

# Errors

All HTTP requests can return user or server errors (HTTP status codes >= 400).
In these cases, you can usually expect a JSON body to come along that has the following
structure:

    {
      "code": "ProgrammaticCode",
      "message: "human consumable message"
    }

The complete list of codes that will be sent are:

- AuthSchemeError
- AuthorizationError
- BadRequestError
- ChecksumError
- ConcurrentRequestError
- ContentLengthError
- ContentMD5MismatchError
- EntityExistsError
- InvalidArgumentError
- InvalidAuthTokenError
- InvalidCredentialsError
- InvalidDurabilityLevelError
- InvalidKeyIdError
- InvalidJobError
- InvalidLinkError
- InvalidLimitError
- InvalidSignatureError
- InvalidUpdateError
- DirectoryDoesNotExistError
- DirectoryExistsError
- DirectoryNotEmptyError
- DirectoryOperationError
- InternalError
- JobNotFoundError
- JobStateError
- KeyDoesNotExistError
- NotAcceptableError
- NotEnoughSpaceError
- LinkNotFoundError
- LinkNotObjectError
- LinkRequiredError
- ParentNotDirectoryError
- PreconditionFailedError
- PreSignedRequestError
- RequestEntityTooLargeError
- ResourceNotFoundError
- RootDirectoryError
- ServiceUnavailableError
- SSLRequiredError
- UploadTimeoutError
- UserDoesNotExistError

Additionally, jobs may emit the above errors, or:

|| **Error name** || **Reason** ||
|| TaskInitError  || Failed to initialize a task (usually a failure to load assets). ||
|| UserTaskError  || User's script returned a non-zero status or one of its processes dumped core. ||

# Directories

## PutDirectory (PUT /:login/stor/[:directory]/:directory)

PutDirectory in the Joyent Manta Storage Service is an idempotent create-or-update operation.  Your private
namespace starts at `/:login/stor`, and you can then create any nested set
of directories or objects underneath that.  To put a directory, simply set the
HTTP Request-URI to the path you want to update, and set the `Content-Type` HTTP
header to `application/json; type=directory`.  There is no request or response
body.  An HTTP status code of `204` is returned on success.

### Sample Request

    $ manta /$MANTA_USER/stor/foo \
        -X PUT \
        -H 'content-type: application/json; type=directory'

    PUT /$MANTA_USER/stor/foo HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    content-type: application/json; type=directory
    Date: Thu, 29 Nov 2012 23:48:00 GMT
    Authorization: $Authorization

    HTTP/1.1 204 No Content
    Date: Thu, 29 Nov 2012 23:48:00 GMT
    Server: Manta
    x-request-id: 3591a050-3a7f-11e2-b95c-a921ce711752
    x-response-time: 16
    x-server-name: 00aa9214-0855-474d-92b5-8f713495b8d7
    Connection: keep-alive

## ListDirectory (GET /:login/stor/:directory)

Lists the contents of a directory. On success you will receive a `\n`
separated stream of JSON objects, where each object represents a single
directory entry.  The content-type will be `application/x-json-stream;
type=directory`.  Each object will have a `type` field, which indicates whether
the entry is a directory or an object. For example (additional newlines added
for clarity):

    {
        "name": "1c1bf695-230d-490e-aec7-3b11dff8ef32",
        "type": "directory",
        "mtime": "2012-09-11T20:28:30Z"
    }

    {
        "name": "695d5de6-45f4-4156-b6b7-3a8d4af89391",
        "etag": "bdf0aa96e3bb87148be084252a059736",
        "size": 44,
        "type": "object",
        "mtime": "2012-09-11T20:28:31Z"
    }

The `type` field indicates the "schema" for each record; the only types are
currently `object` and `directory`. Both have a `name` (filename), `type`
(already described) and an `mtime`, which is an ISO8601 timestamp of the last
update time.  Additionally, records of type `object` have a `size`
(content-length) and `etag` (for conditional requests).

You will get back entries in blocks of *256* (you can opt for less, or more by
setting the `limit` parameter on the query string).  You can choose where to
start the next listing by using the `marker` query parameter.  You'll get the
*total* number of records in the `result-set-size` header. The service lists objects
in alphabetical order (UTF-8 collation).

### Query Parameters

||**Name**||**Description**||
||limit||limits the number of records to come back (default and max is 1000)||
||marker||key name at which to start the next listing||

### Returns

A stream of JSON objects, one record for each child.

    $ manta /$MANTA_USER/stor/

    GET /$MANTA_USER/stor HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    Authorization: $Authorization

    HTTP/1.1 200 OK
    Content-Type: application/x-json-stream; type=directory
    Result-Set-Size: 1
    Date: Fri, 30 Nov 2012 00:24:28 GMT
    Server: Manta
    x-request-id: 4db4ba00-3a84-11e2-b95c-a921ce711752
    x-response-time: 8
    x-server-name: 00aa9214-0855-474d-92b5-8f713495b8d7
    Connection: keep-alive
    Transfer-Encoding: chunked

    {"name":"foo","type":"directory","mtime":"2012-11-29T23:48:00Z"}

## DeleteDirectory (DELETE /:login/stor/:directory)

Deletes a directory. The directory must be empty.  There is no response data
from this request.  On success an HTTP `204` is returned;

### Sample Request

    $ manta /$MANTA_USER/stor/foo -X DELETE

    DELETE /$MANTA_USER/stor/foo HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    Date: Fri, 30 Nov 2012 00:31:00 GMT
    Authorization: $Authorization

    HTTP/1.1 204 No Content
    Last-Modified: Thu, 29 Nov 2012 23:48:00 GMT
    Date: Fri, 30 Nov 2012 00:31:00 GMT
    Server: Manta
    x-request-id: 371e7320-3a85-11e2-8d0c-417db534d10b
    x-response-time: 15
    x-server-name: fb07e9ec-5137-418e-aff2-01d00aff1a49
    Connection: keep-alive

# Objects

## PutObject (PUT /:login/stor/[:directory]/:object)

Creates or overwrites an object.  You specify the path to an object just as you
would on a traditional file system, and the parent must be a directory.  The service
will do no interpretation of your data.  Specifically, that means your data is
treated as an opaque byte stream, and you will receive back *exactly* what you
upload.  On success an HTTP `204` is returned.

By default, The service  will store two copies of your data on two physical servers in
two different datacenters; note that each physical server is configured with
RAID-Z, so a disk drive failure does not impact your durability or availability.
You can increase (or decrease) the number of copies of your object with the
`durability-level` header.

You should always specify a `Content-Type` header, which will be stored and
returned back (HTTP content-negotiation will be handled).   If you do not
specify one, the default is `application/octet-stream`.

You should specify a `Content-MD5` header; if you do the service will validate that
the content uploaded matches the value of the header.  Even if you do not include
one, successful responses will include a `computed-md5` header. This is the MD5
checksum that Manta calculated when ingesting your object.

The service is able to provide test/set semantics for you if you use HTTP conditional
request semantics (e.g., `If-Match` or `If-Modified-Since`).

Cross-Origin Resource Sharing [CORS](http://www.w3.org/TR/cors/) headers are
saved as appropriate; preflighted requests are supported by sending a list of
values in `access-control-allow-origin`.

By default, the service will store 2 copies of your object; this can be changed with
the `durability-level` header.

Note that if you are using "streaming uploads" (transfer-encoding chunked), you
are either subject to the default object size in the service (5Gb), or you need to
"guess" how big your object is going to be by overriding with the HTTP header
`max-content-length`.  Either way, the service will track how many bytes were
_actually_  sent and record that. Subsequent GET requests will return the actual
length.  If you're not streaming, just set `content-length` (as you normally
would).

Lastly, you can store custom headers with your object (e.g. "tags") by prefixing
them with `m-`.  So for example, including the header `m-local-user: foo` with
an object will allow you to track that some local user to your application
created the object.  You are allowed up to 4 Kb of header data.

### Sample Request

    $ manta /$MANTA_USER/stor/foo.json -X PUT -H 'content-type: application/json' \
            -d '{"hello": "world"}'

    PUT /$MANTA_USER/stor/foo.json HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    content-type: application/json
    Date: Fri, 30 Nov 2012 00:55:06 GMT
    Authorization: $Authorization
    Content-Length: 18

    {"hello": "world"}


    HTTP/1.1 204 No Content
    Etag: f501ffd1-3e28-49a8-aaa5-2c1555c34ce0
    Last-Modified: Fri, 30 Nov 2012 00:55:06 GMT
    Date: Fri, 30 Nov 2012 00:55:06 GMT
    Server: Manta
    x-request-id: 952ddbb0-3a88-11e2-b95c-a921ce711752
    x-response-time: 18
    x-server-name: 00aa9214-0855-474d-92b5-8f713495b8d7
    Connection: keep-alive

## PutMetadata (PUT /:login/stor/[:directory]/:object?metadata=true)

PutMetadata allows you to overwrite the HTTP headers for an already existing
object, without changing the data. Note this is an idempotent "replace"
operation, so you must specify the complete set of HTTP headers you want stored
on each request.

You cannot change "critical" headers:

- Content-Length
- Content-MD5
- Durability-Level

### Sample Request

    $ manta /$MANTA_USER/stor/foo.json?metadata=true -X PUT \
            -H 'content-type: application/json'

    PUT /$MANTA_USER/stor/foo.json?metadata=true HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    content-type: application/json
    m-foo: bar
    Date: Fri, 30 Nov 2012 00:55:06 GMT
    Authorization: $Authorization


    HTTP/1.1 204 No Content
    Etag: f501ffd1-3e28-49a8-aaa5-2c1555c34ce0
    Last-Modified: Fri, 30 Nov 2012 00:55:06 GMT
    Date: Fri, 30 Nov 2012 00:55:06 GMT
    Server: Manta
    x-request-id: caf3ccbb-5138-43c1-9c27-2bf8f13f76f3
    x-response-time: 18
    x-server-name: 00aa9214-0855-474d-92b5-8f713495b8d7
    Connection: keep-alive

## GetObject (GET /:login/stor/[:directory]/:object)

Retrieves an object from the service.  On success an HTTP `200` is returned along with
your content and metadata (HTTP headers).

### Sample Request

    $ manta /$MANTA_USER/stor/foo.json

    GET /$MANTA_USER/stor/foo.json HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    Date: Fri, 30 Nov 2012 00:59:17 GMT
    Authorization: $Authorization


    HTTP/1.1 200 OK
    Etag: f501ffd1-3e28-49a8-aaa5-2c1555c34ce0
    Last-Modified: Fri, 30 Nov 2012 00:55:06 GMT
    Content-Length: 18
    Content-Type: application/json
    Content-MD5: Sd/dVLAcvNLSq16eXua5uQ==
    Date: Fri, 30 Nov 2012 00:59:18 GMT
    Server: Manta
    x-request-id: 2af906b0-3a89-11e2-8d0c-417db534d10b
    x-response-time: 7
    x-server-name: fb07e9ec-5137-418e-aff2-01d00aff1a49
    Connection: keep-alive

    {"hello": "world"}

## DeleteObject (DELETE /:login/stor/[:directory]/:object)

Deletes an object from the service. On success an HTTP `204` is returned.

### Sample Request

    $ manta /$MANTA_USER/stor/foo.json -X DELETE

    DELETE /$MANTA_USER/stor/foo/bar.json HTTP/1.1
    User-Agent: curl/7.21.2 (i386-pc-solaris2.11) libcurl/7.21.2 OpenSSL/0.9.8w zlib/1.2.3
    Host: us-east.manta.joyent.com
    Accept: */*
    Date: Fri, 30 Nov 2012 01:01:33 GMT
    Authorization: $Authorization


    HTTP/1.1 204 No Content
    Etag: f501ffd1-3e28-49a8-aaa5-2c1555c34ce0
    Last-Modified: Fri, 30 Nov 2012 00:55:06 GMT
    Date: Fri, 30 Nov 2012 01:01:33 GMT
    Server: Manta
    x-request-id: 7b872080-3a89-11e2-8d0c-417db534d10b
    x-response-time: 8
    x-server-name: fb07e9ec-5137-418e-aff2-01d00aff1a49
    Connection: keep-alive

# SnapLinks

## PutSnapLink (PUT /:login/stor/[:directory]/:link)

Creates a SnapLink to an object. On success, an HTTP `204` is returned.  Specify
the "source" object by sending the path in the `Location` header.

### Sample Request

First make an object, then create a link:

    $ manta /$MANTA_USER/stor/foo.json -X PUT -H 'content-type: application/json' \
            -d '{"hello": "world"}'

    $ manta /$MANTA_USER/stor/foo.json.2 -X PUT \
         -H 'content-type: application/json; type=link' \
         -H 'Location: /$MANTA_USER/stor/foo.json'

    PUT /$MANTA_USER/stor/foo.json.2 HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    content-type: application/json; type=link
    Location: /$MANTA_USER/stor/foo.json
    Date: Fri, 30 Nov 2012 01:14:17 GMT
    Authorization: $Authorization


    HTTP/1.1 204 No Content
    Etag: 447a76c7-c1ff-4613-b64a-02c059a02d92
    Last-Modified: Fri, 30 Nov 2012 01:13:59 GMT
    Date: Fri, 30 Nov 2012 01:14:18 GMT
    Server: Manta
    x-request-id: 4391f400-3a8b-11e2-928b-1324a0f99d70
    x-response-time: 17
    x-server-name: ef69fb79-d88b-4c9d-be81-de55dd60da5b
    Connection: keep-alive

# Jobs

## CreateJob (POST /:login/jobs)

Submits a new job to be executed. This call is not idempotent, so calling it
twice will create two jobs.  On success, an HTTP `201` is returned with the URI
for the job in the `Location` header.

### Inputs

The body of this request must be a JSON document with the following properties:

||**Name**||**JS Type**||**Description**||
||name||String||_(optional)_ An arbitrary name for this job||
||phases||Array||*(required)* tasks to execute as part of this job||

`phases` must be an `Array` of `Object`, where objects have the following
properties:

||**Name**||**JS Type**||**Description**||
||type||String||_(optional)_ one of: `map` or `reduce`||
||assets||Array(String)||_(optional)_ an array of objects to be placed *in* your compute zones||
||exec||String||*(required)* the actual (shell) statement to execute||
||init||String||*(required)* shell statement to execute in each compute zone before any tasks are executed ||
||count||Number||_(optional)_ an optional number of reducers for this phase (reduce-only): default is `1`||
||memory||Number||_(optional)_ an optional amount of DRAM to give to your compute zone (MB)||
||disk||Number||_(optional)_ an optional amount of disk space to give to your compute zone (GB)||

* `count` has a minimum of `1` (default), and a maximum of `1024`.
* `memory` must be one of the following: 256, 512, 1024, 2048, 4096, 8192
* `disk` must be one of the following: 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024
* `exec` may be any valid shell command, including pipelines and other shell
  syntax.  You can also execute programs stored in the service by including them in
  "assets" and referencing them as /assets/$manta\_path.
* `init` has the same constraints as `exec`, but is executed in each compute
  zone before any tasks run

### Sample Request

    $ cat job.json
    {
        "name": "word count",
        "phases": [ {
            "exec": "wc"
        }, {
            "type": "reduce",
            "exec": "awk '{ l += $1; w += $2; c += $3 } END { print l, w, c }'"
        } ]
    }
    $ manta /$MANTA_USER/jobs -X POST -H 'content-type: application/json' --data-binary @job.json

    POST /$MANTA_USER/jobs HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    content-type: application/json
    Date: Fri, 30 Nov 2012 01:25:28 GMT
    Authorization: $Authorization
    Content-Length: 187


    HTTP/1.1 201 Created
    Content-Length: 0
    Location: /$MANTA_USER/jobs/a62ba79e-4d5b-4773-bff9-ecae0fe30dfa
    Date: Fri, 30 Nov 2012 01:25:28 GMT
    Server: Manta
    x-request-id: d3534b60-3a8c-11e2-8d0c-417db534d10b
    x-response-time: 10
    x-server-name: fb07e9ec-5137-418e-aff2-01d00aff1a49
    Connection: keep-alive

## AddJobInputs (POST /:login/jobs/:id/live/in)

Submits inputs to an already created job, as created by [CreateJob](#CreateJob).

Inputs are object names, and are fed in as a `\n` separated stream (content-type
as `text/plain`). Inputs will be processed as they are received.

An HTTP `204` is returned on success.

### Sample Request

*note:* all lines in `inputs.txt` have a trailing `\n`

    $ cat inputs.txt
    /$MANTA_USER/stor/words
    /$MANTA_USER/stor/words.2
    /$MANTA_USER/stor/words.3
    $ manta /$MANTA_USER/jobs/a62ba79e-4d5b-4773-bff9-ecae0fe30dfa/live/in -X POST \
            -H content-type:text/plain --data-binary @inputs.txt

    POST /$MANTA_USER/jobs/a62ba79e-4d5b-4773-bff9-ecae0fe30dfa/live/in HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    content-type: text/plain
    Date: Tue, 04 Dec 2012 16:20:17 GMT
    Authorization: $Authorization
    Content-Length: 55


    HTTP/1.1 204 No Content
    Connection: close
    Date: Tue, 04 Dec 2012 16:20:17 GMT
    Server: Manta
    x-request-id: 7dc1d950-3e2e-11e2-9dd2-2bc6e71ae3bc
    x-response-time: 72
    x-server-name: 27721605-e942-4dfb-b01d-9b9ac9847dde

## EndJobInput (POST /:login/jobs/:id/live/in/end)

This "closes" input for a job, and will finalize the job.  If there are
reducers, you likely won't see output until after this is called.  There is no
input to this API.  On success an HTTP `202` is returned.


### Sample Request

    $ manta /$MANTA_USER/jobs/a62ba79e-4d5b-4773-bff9-ecae0fe30dfa/live/in/end -X POST

    POST /$MANTA_USER/jobs/a62ba79e-4d5b-4773-bff9-ecae0fe30dfa/live/in/end HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    Date: Tue, 04 Dec 2012 16:26:24 GMT
    Authorization: $Authorization


    HTTP/1.1 202 Accepted
    Connection: close
    Date: Tue, 04 Dec 2012 16:26:24 GMT
    Server: Manta
    x-request-id: 58b3f570-3e2f-11e2-be8f-87b1bf6e7c11
    x-response-time: 27
    x-server-name: 8469d848-aee5-4562-957a-38b326ad454f

## CancelJob (POST /:login/jobs/:id/live/cancel)

This cancels a job from doing any further work. Cancellation is asynchronous and
"best effort"; there is no guarantee the job will actually stop. For example,
short jobs where input is already closed will likely still run to completion.
This is however useful when:

- input is still open
- you have a long-running job

On success an HTTP `202` is returned.

### Sample Request

    $ manta /$MANTA_USER/jobs/495a3099-0394-4904-a17c-5317b5b2162c/live/cancel -X POST

    POST /$MANTA_USER/jobs/495a3099-0394-4904-a17c-5317b5b2162c/live/cancel HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    content-type: application/json
    Date: Tue, 04 Dec 2012 16:34:17 GMT
    Authorization: $Authorization

    HTTP/1.1 202 Accepted
    Connection: close
    Date: Tue, 04 Dec 2012 16:34:18 GMT
    Server: Manta
    x-request-id: 72e68b00-3e30-11e2-be8f-87b1bf6e7c11
    x-response-time: 15
    x-server-name: 8469d848-aee5-4562-957a-38b326ad454f


## ListJobs (GET /:login/jobs)

Returns the list of jobs you currently have.  This is a streaming JSON payload
that will be identical to listing a directory.  Note you can filter the set of
jobs down to only `live` jobs by using the query parameter `?state=running`.

On success, an HTTP `200` is returned with a stream of `job` objects
(see [CreateJob](#CreateJob)).  Note a content-length header will not be sent
back.

### Sample Request

Additional newlines added for clarity:

    $ manta /$MANTA_USER/jobs | json -ga

    GET /$MANTA_USER/jobs HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    Date: Tue, 04 Dec 2012 16:35:56 GMT
    Authorization: $Authorization


    HTTP/1.1 200 OK
    Connection: close
    Date: Tue, 04 Dec 2012 16:35:56 GMT
    Server: Manta
    x-request-id: ada10720-3e30-11e2-9dd2-2bc6e71ae3bc
    x-response-time: 5
    x-server-name: 27721605-e942-4dfb-b01d-9b9ac9847dde
    Content-Type: application/x-json-stream; type=directory
    Transfer-Encoding: chunked

    {"name":"a1782c7d-529e-4a1a-980d-dfdc805c3124","type":"directory","mtime":"2012-11-29T23:48:00Z"}
    {"name":"980c514f-bc59-4fb4-b357-d03f9067dffa","type":"directory","mtime":"2012-11-29T23:48:00Z"}


## GetJob (GET /:login/jobs/:id/live/status)

Gets the high-level job container object for a given id. Content-type will be
`application/json`.

An HTTP `204` is returned on success.

### Sample Request

Additional newlines added for clarity:

    $ manta /$MANTA_USER/jobs/a62ba79e-4d5b-4773-bff9-ecae0fe30dfa/live/status | json

    GET /$MANTA_USER/jobs/a62ba79e-4d5b-4773-bff9-ecae0fe30dfa/live/status HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    Date: Tue, 04 Dec 2012 16:41:29 GMT
    Authorization: $Authorization


    HTTP/1.1 200 OK
    Connection: close
    Content-Type: application/json
    Content-Length: 325
    Content-MD5: 6txxiXII5zL+8Ombjc3CwA==
    Date: Tue, 04 Dec 2012 16:41:30 GMT
    Server: Manta
    x-request-id: 7453ba20-3e31-11e2-acef-af63e7e14029
    x-response-time: 5
    x-server-name: c5df6b59-f27d-4f02-b6db-0e6b044b0d79

    {
      "id": "a62ba79e-4d5b-4773-bff9-ecae0fe30dfa",
      "name": "word count",
      "state": "done",
      "cancelled": false,
      "inputDone": true,
      "timeCreated": "2012-12-04T16:23:13.641Z",
      "timeDone": "2012-12-04T16:26:24.998Z",
      "phases": [
        {
          "exec": "wc",
          "type": "map"
        },
        {
          "type": "reduce",
          "exec": "awk '{ l += $1; w += $2; c += $3 } END { print l, w, c }'"
        }
      ]
    }


## GetJobOutput (GET /:login/jobs/:id/live/out)

Returns the current "live" set of outputs from a job.  Think of this like
`tail -f`.  The objects are returned as a stream, with an HTTP status
code of `200` on success.


### Sample Request

    $ manta /$MANTA_USER/jobs/a62ba79e-4d5b-4773-bff9-ecae0fe30dfa/live/out

    GET /$MANTA_USER/jobs/a62ba79e-4d5b-4773-bff9-ecae0fe30dfa/out HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    Date: Tue, 04 Dec 2012 16:47:14 GMT


    HTTP/1.1 200 OK
    Connection: close
    Content-Type: text/plain
    Result-Set-Size: 1
    Date: Tue, 04 Dec 2012 16:47:17 GMT
    Server: Manta
    x-request-id: 43745300-3e32-11e2-9dd2-2bc6e71ae3bc
    x-response-time: 18
    x-server-name: 27721605-e942-4dfb-b01d-9b9ac9847dde
    Transfer-Encoding: chunked

    /$MANTA_USER/jobs/a62ba79e-4d5b-4773-bff9-ecae0fe30dfa/stor/reduce.1.652bdcac-28c8-4e07-b887-91f513966e8b

At this point you could retrieve that object (only data shown):

    $ manta /$MANTA_USER/jobs/a62ba79e-4d5b-4773-bff9-ecae0fe30dfa/stor/reduce.1.652bdcac-28c8-4e07-b887-91f513966e8b
    75432 75432 620022


## GetJobInput (GET /:login/jobs/:id/live/in)

Submitted input objects are available while a job is running as a `\n` separated
text stream (the output will simply be the object's name). The objects are
returned as a stream, with an HTTP status code of `200` on success.

### Sample Request

    $ manta /$MANTA_USER/jobs/a62ba79e-4d5b-4773-bff9-ecae0fe30dfa/live/in

    GET /$MANTA_USER/jobs/a62ba79e-4d5b-4773-bff9-ecae0fe30dfa/live/in HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    Date: Tue, 04 Dec 2012 16:52:05 GMT
    Authorization: $Authorization


    HTTP/1.1 200 OK
    Connection: close
    Content-Type: text/plain
    Result-Set-Size: 3
    Date: Tue, 04 Dec 2012 16:52:05 GMT
    Server: Manta
    x-request-id: ef3d8710-3e32-11e2-9dd2-2bc6e71ae3bc
    x-response-time: 13
    x-server-name: 27721605-e942-4dfb-b01d-9b9ac9847dde
    Transfer-Encoding: chunked

    /$MANTA_USER/stor/words
    /$MANTA_USER/stor/words.2
    /$MANTA_USER/stor/words.3

## GetJobFailures (GET /:login/jobs/:id/live/fail)

Returns the current "live" set of failures from a job.  Think of this like
`tail -f`.  The objects are returned as a stream, with an HTTP status
code of `200` on success.

### Sample Request

    $ manta /$MANTA_USER/jobs/d558881d-a02f-43d3-846b-84771b6bf1ed/live/fail

    GET /$MANTA_USER/jobs/d558881d-a02f-43d3-846b-84771b6bf1ed/live/fail HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    Date: Tue, 04 Dec 2012 16:56:14 GMT
    Authorization: $Authorization


    HTTP/1.1 200 OK
    Connection: close
    Content-Type: text/plain
    Date: Tue, 04 Dec 2012 16:56:15 GMT
    Server: Manta
    x-request-id: 83e98670-3e33-11e2-be8f-87b1bf6e7c11
    x-response-time: 6
    x-server-name: 8469d848-aee5-4562-957a-38b326ad454f
    Transfer-Encoding: chunked

    /$DIFFERENT_MANTA_USER/stor/words
    /$MANTA_USER/stor/words.4

## GetJobErrors (GET /:login/jobs/:id/live/err)

Returns the current "live" set of errors from a job.  Think of this like
`tail -f`.  The objects are returned as a stream, with an HTTP status code of
`200` on success.  The body is a JSON stream
`application/x-json-stream; type=job-error`.

||**Name**||**Type**||**Description**||
||taskId||String||unique identifier for the task||
||phaseNum||Number||phase number of the failure||
||what||String||a human readable summary of what failed||
||code||String||programmatic error code||
||message||String||human readable error message||
||server||String||logical server name where the task was executed||
||machine||String||zonename where the task was executed||
||input||String||*(optional)* object being processed when the map task failed||
||p0input||String||*(optional)* phase 0 input object, returned if not preceded by reduce phase||
||core||String||*(optional)* core file name, if any process in the task dumped core||
||stderr||String||*(optional)* a key that saved the stderr for the given command||

### Sample Request

Additional newlines added for clarity:

    $ manta /$MANTA_USER/jobs/d558881d-a02f-43d3-846b-84771b6bf1ed/live/err | json -ga

    GET /$MANTA_USER/jobs/d558881d-a02f-43d3-846b-84771b6bf1ed/live/err HTTP/1.1
    Host: us-east.manta.joyent.com
    Accept: */*
    Date: Tue, 04 Dec 2012 17:14:43 GMT
    Authorization: $Authorization

    HTTP/1.1 200 OK
    Connection: close
    Content-Type: application/x-json-stream; type=job-error
    Date: Tue, 04 Dec 2012 17:14:44 GMT
    Server: Manta
    x-request-id: 190329d0-3e36-11e2-acef-af63e7e14029
    x-response-time: 13
    x-server-name: c5df6b59-f27d-4f02-b6db-0e6b044b0d79
    Transfer-Encoding: chunked

    {
      "id": "d558881d-a02f-43d3-846b-84771b6bf1ed",
      "phase": 0,
      "what": "phase 0: map key \"/$DIFFERENT_MANTA_USER/stor/words\"",
      "code": "EJ_ACCES",
      "message": "permission denied: \"/$DIFFERENT_MANTA_USER/stor/words\"",
      "key": "/$DIFFERENT_MANTA_USER/stor/words"
    }
    {
      "id": "d558881d-a02f-43d3-846b-84771b6bf1ed",
      "phase": 0,
      "what": "phase 0: map key \"/$MANTA_USER/stor/words.4\"",
      "code": "EJ_NOENT",
      "message": "no such object: \"/$MANTA_USER/stor/words.4\"",
      "key": "/$MANTA_USER/stor/words.4"
    }
