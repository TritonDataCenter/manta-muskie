<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2017, Joyent, Inc.
-->


# Deployment Architecture

Muskie is implemented as a [restify](https://github.com/restify/node-restify)
service, and is deployed highly redundantly, both with a zone and in many
zones.  In a typical production deployment, there are 16 muskie processes
per zone, with an [HAProxy](http://www.haproxy.org/) instance sitting in
front of them (in TCP mode).  All logging from muskies in a production
setting is done through [bunyan](https://github.com/trentm/node-bunyan) to
`/var/log/muskie.log`.  The HAProxy listens on both 80 and 81, where 80 is
meant to mean "secure" requests, which either means non-TLS from the "internal"
network (e.g., marlin) or TLS requests from the world.  81 is used to serve
up anonymous traffic, which is only allowed in muskie to support anonymous GETs
(signed URLs and `GET /:user/public`).

# Dependencies

At runtime, muskie depends on [mahi](https://github.com/joyent/mahi) for
authentication/authorization, the
[electric-moray](https://github.com/joyent/electric-moray) stack for
object traffic, and [mako](https://github.com/joyent/mako) for writing objects.
The same [moray](https://github.com/joyent/moray) instance that
[marlin](https://github.com/joyent/manta-marlin) uses is contacted for all APIs
involving job management.  Lastly,
[medusa](https://github.com/joyent/manta-medusa) is needed for `mlogin`
functionality.

## Storage Picker

One additional dependency in muskie is on a Moray shard (by convention the same
one that marlin uses), to store the storage node `statvfs` information.  Muskie
periodically refreshes this and *always* selects storage nodes from the local
cache on writes, as opposed to hitting Moray directly for this purpose.  This is
purely in-memory so all muskie *processes* have a cached copy.

# Monitoring

There is a second server running in each muskie process to
facilitate monitoring.  This is accessible at `http_insecure_port + 800`.  For
example, the first (of 16 in production) muskie process within a zone usually
runs on port 8081, so the monitoring server would be accesible on port 8881 from
both the `manta` and `admin` networks.  The monitoring server exposes
Kang debugging information and Prometheus application metrics.

## Kang

Kang debugging information is accessible from the route `GET /kang/snapshot`.
For more information on Kang, see the documentation on
[GitHub](https://github.com/davepacheco/kang/blob/master/README.md).

## Metrics

Application metrics can be retrieved from the route `GET /metrics`.  The metrics
are returned in [Prometheus](https://prometheus.io/) v0.0.4 text format.

The following metrics are collected:

- Time-to-first-byte latency for all requests
- End-to-end latency for all requests
- Count of requests completed
- Count of bytes streamed to and from storage

Each of the metrics returned include the following metadata labels:

- Datacenter name (i.e. us-east-1)
- CN UUID
- Zone UUID
- PID
- Operation (i.e. 'putobject')
- Method (i.e. 'PUT')
- HTTP response status code (i.e. 203)

The metric collection facility provided is intended to be consumed by a
monitoring service like a Prometheus or InfluxDB server.

# Logic

## PutObject

On a `PutObject` request, muskie will, in order:

- Lookup metadata for parent directory and current key (if overwrite)
- Ensure the parent directory exists
- If etags, enforce etag
- select random makos
- stream bytes
- save metadata back into electric-moray

Note that there is a slight race where users could theoretically be writing an
object while they delete the parent directory, leading to an "orphaned" object,
but this could only happen if there were no objects in the directory, and an
upload was happening while the user deleted the parent.

### Selecting Storage Nodes

Probably the most interesting aspect of PutObject that's hard to figure out from
code inspection is how storage node selection works.  In essence, we always
select random "stripes" of hosts (where a stripe is as wide as
`durability-level`), across datacenters (if a multi-DC deployment) that have
heartbeated in the last N seconds and have enough space.  Muskie then
synchronously starts requests to *all* of the servers in the stripe, and if
*any* fail, it abandons and moves to the next stripe.  Assuming 1 of the stripes
has 100% connect success rate, muskie completes the request. If all the stripes
are exhausted, muskie returns an HTTP 503 error code.  To make a practical
example, assume the following topology of storage nodes:

```
DC1  | DC2 | DC3
-----+-----+-----
A,D,G|B,E,H|C,F,I
```

On a Put request, muskie might select the following configurations (totally
random):

```
1. B,C
2. I,A,
3. E,D
4. G,F
```

And supposing `C` and `A` are somehow down, then muskie would end up writing the
user data to pair `E,D`.

## GetObject

On a GET request, muskie will go fetch the metadata for the record from
electric-moray and then start requests to *all* of the storage nodes that hold
the object.  The first one to respond is the one that ultimately streams back to
the user, and the other requests are abandoned.

## DeleteObject

Deletion of objects is actually very simple - muskie simply deletes the metadata
record in moray.  Actual blobs of data are asynchronously garbage collected.
See [manta-mola](https://github.com/joyent/manta-mola) for more information.

## PutDirectory

Creating or updating a directory in Manta can be thought of a special case of
creating an object where all aspects of streaming the user byte stream are
ignored. But otherwise muskie does the same metadata lookups and enforcement,
and then simply saves the directory record into Moray.

Additionally, directories are setup with postgres triggers to maintain a count
of entries (this is returned in the `result-set-size` HTTP header).  See
`moray.js` in [node-libmanta](https://github.com/joyent/node-libmanta) for the
actual meat, but this allows muskie to not ask moray to do `SELECT count(*)`,
which is known to be very slow at scale in postgres.

## GetDirectory

Listing a directory in muskie is just a bounded listing of entries in moray
(e.g. a `findobjects`) request.  The `manta_directory_counts`
(trigger-maintained) bucket is used to fill in `result-set-size`.

## DeleteDirectory

Deletion of a directory simply ensures there are no children and then drops the
dirent; again there is a tiny race here as this spans shards.
