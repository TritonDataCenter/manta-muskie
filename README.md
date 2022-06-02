<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2020 Joyent, Inc.
    Copyright 2022 MNX Cloud, Inc.
-->

# manta-muskie: The Manta WebAPI

This repository is part of the Joyent Manta project.  For contribution
guidelines, issues, and general documentation, visit the main
[Manta](http://github.com/TritonDataCenter/manta) project page.

manta-muskie holds the source code for the Manta WebAPI, otherwise known as
"the front door". API documentation is in [docs/index.md](./docs/index.md). Some
design documentation (possibly quite dated) is in
[docs/internal](./docs/internal). Developer notes are in this README.

## Active Branches

There are currently two active branches of this repository, for the two
active major versions of Manta. See the [mantav2 overview
document](https://github.com/TritonDataCenter/manta/blob/master/docs/mantav2.md) for
details on major Manta versions.

* [`master`](../../tree/master/) - For development of mantav2, the latest
  version of Manta.
* [`mantav1`](../../tree/mantav1/) - For development of mantav1, the long
  term support maintenance version of Manta.

## Testing

Muskie tests use node-tap. Test files are all named "test/**/*.test.js".
Tests are divided into:

1. Unit tests (test/unit/*.test.js). These can be run from either a git
   clone (`make test-unit` is hooked up to do this) or inside a deployed
   muskie (aka "webapi") instance.

2. Integration tests (test/integration/*.test.js). These must be run from
   a deployed muskie instance.

Each test file must be written to run independently. This allows running
tests in parallel and being able to understand what a test is doing without
assumed external setup steps.

To run unit tests in a git clone:

    make test-unit

To run all tests on a (non-production) muskie (aka "webapi") instance:

    ssh DC-HEADNODE-GZ      # login to the headnode
    manta-login webapi      # login to a webapi instance
    /opt/smartdc/muskie/test/runtests

"Runtests" by default shows a compact results summary. Full TAP output is
written to "/opt/smartdc/muskie/test.tap". See the comment in
[runtests](./test/runtests) for various use cases for running the tests -- e.g.
running individual test files, forcing TAP output.

To run the muskie test suite in the internal Joyent "nightly-2" DC run:

    https://jenkins.joyent.us/view/nightly/job/nightly-ad-hoc-single-stage/build?delay=0sec
        TARGET_RIG: nightly-2
        CMD:        stage-test-manta-muskie

### Cleaning up after tests

Many of the integration tests require actual user accounts with which to test.
The code to handle this is in "test/helper.js#ensureTestAccounts" -- the
account logins are prefixed with "muskietest_", account data is cached in
"/var/db/muskietest".

For faster re-runs of the test suite, these accounts are *not* deleted after a
test run. Generally this should be fine (the muskie integration tests shouldn't
be run in a *production* datacenter). However, if necessary, you can delete
the muskie test accounts fully by:

1. Copying the "test/sdc-useradm-rm.sh" tool to "/var/tmp" in the global zone;
   and

2. Running the following from the headnode global zone:

        function reset_muskie_test_accounts {
            sdc-useradm search muskietest_ -H -o login | while read login; do
                I_REALLY_WANT_TO_SDC_USERADM_RM=1 /var/tmp/sdc-useradm-rm.sh $login
            done
            manta-oneach -s authcache 'svcadm restart mahi'
            manta-oneach -s webapi 'svcadm restart svc:/manta/application/muskie:muskie-*'
        }
        reset_muskie_test_accounts

### Dev Cycle

If you are changing node.js code only, you may benefit from the
"./tools/rsync-to" script to copy local dev changes to a deployed muskie on the
headnode of a development Manta (then re-run the test suite):

    vi ...                      # make a code change
    ./tools/rsync-to HEADNODE   # where HEADNODE is an ssh name to the dev headnode

For larger changes, refer to [the Operator
Guide](https://github.com/TritonDataCenter/manta/blob/master/docs/operator-guide/maintenance.md#upgrading-manta-components)
for upgrading a webapi instance in a Manta setup.

## Metrics

Muskie exposes metrics via [node-artedi](https://github.com/TritonDataCenter/node-artedi).
See the [design](./docs/internal/design.md) document for more information about
the metrics that are exposed, and how to access them. For development, it is
probably easiest to use `curl` to scrape metrics:

    curl http://localhost:8881/metrics

Notably, some metadata labels are not being collected due to their potential
for high cardinality.  Specifically, remote IP address, object owner, and caller
username are not collected.  Metadata labels that have a large number of unique
values cause memory strain on metric client processes (muskie) as well as
metric servers (Prometheus).  It's important to understand what kind of an
effect on the entire system the addition of metrics and metadata labels can have
before adding them. This is an issue that would likely not appear in a
development or staging environment.

## Notes on DNS and service discovery

Like most other components in Triton and Manta, Muskie (deployed with service
name "webapi") uses [Registrar](https://github.com/TritonDataCenter/registrar/) to
register its instances in internal DNS so that other components can find them.
The general mechanism is [documented in detail in the Registrar
README](https://github.com/TritonDataCenter/registrar/blob/master/README.md).  There are
some quirks worth noting about how Muskie uses this mechanism.

First, while most components use local config-agent manifests that are checked
into the component repository (e.g., `$repo_root/sapi_manifest/registrar`),
Muskie still uses an application-provided SAPI manifest.  See
[MANTA-3173](https://smartos.org/bugview/MANTA-3173) for details.

Second, Muskie registers itself with DNS domain `manta.$dns_suffix` (where
`$dns_suffix` is the DNS suffix for the whole deployment).  This is the same DNS
name that the "loadbalancer" service uses for its instances.  If you look up
`manta.$dns_suffix` in a running Manta deployment, you get back the list of
"loadbalancer" instances -- not any of the "webapi" (muskie) instances.  That's
because "loadbalancer" treats this like an ordinary service registration with a
service record at `manta.$dns_suffix` and `load_balancer` records underneath
that that represent individual instances of the `manta.$dns_suffix` service, but
"webapi" registers `host` records underneath that domain.  As the
above-mentioned Registrar docs explain, `host` records are not included in DNS
results when a client queries for the service DNS name.  They can only be used
to query for the IP address of a specific instance.  **The net result of all
this is that you can find the IP address of a Muskie zone whose zonename you
know by querying for `$zonename.manta.$dns_suffix`, but there is no way to
enumerate the Muskie instances using DNS, nor is there a way to add that without
changing the DNS name for webapi instances, which would be a flag day for
Muppet.**  (This may explain why [muppet](https://github.com/TritonDataCenter/muppet) is a
ZooKeeper consumer rather than just a DNS client.)

## Dtrace Probes

Muskie has two dtrace providers. The first, `muskie`, has the following probes:

* `client_close`: `json`. Fires if a client uploading an object or part closes
  before data has been streamed to mako. Also fires if the client closes the
  connection while the stream is in progress. The argument json object has the
  following format:

        {
          id: restify uuid, or x-request-id/request-id http header (string)
          method: request http method (string)
          headers: http headers specified by the client (object)
          url: http request url (string)
          bytes_sent: number of bytes streamed to mako before client close (int)
          bytes_expected: number of bytes that should have been streamed (int)
        }

* `socket_timeout`: `json`. Fires when the timeout limit is reached on a
  connection to a client. This timeout can be configured either by setting the
  `SOCKET_TIMEOUT` environment variable. The default is 120 seconds. The object
  passed has the same fields to the `client_close` dtrace probe, except for the
  `bytes_sent` and `bytes_expected`. These parameters are only present if muskie
  is able to determine the last request sent on this socket.

The second provider, `muskie-throttle`, has the following probes, which will not
fire if the throttle is disabled:

* `request_throttled`: `int`, `int`, `char *`, `char *` - slots occupied, queued
  requests, url, method. Fires when a request has been throttled.
* `request_handled`: `int`, `int`, `char *`, `char *` - slots occupied, queued
  requests, url, method. Fires after a request has been handled.
  Internally, the muskie throttle is implemented with a vasync-queue. A "slot"
  in the above description refers to one of `concurrency` possible spaces
  allotted for concurrently scheduled request-handling callbacks. If all slots
  are occupied, incoming requests will be "queued", which indicates that they
  are waiting for slots to free up.
* `queue_enter`: `char *` - restify request uuid. This probe fires as a request
  enters the queue.
* `queue_leave`: `char *` - restify request uuid. This probe fires as a request
  is dequeued, before it is handled. The purpose of these probes is to make it
  easy to write d scripts that measure the latency impact the throttle has on
  individual requests.

The script `bin/throttlestat.d` is implemented as an analog to `moraystat.d`
with the `queue_enter` and `queue_leave` probes. It is a good starting point for
gaining insight into both how actively a muskie process is being throttled and
how much stress it is under.

The throttle probes are provided in a separate provider to prevent coupling the
throttle implementation with muskie itself. Future work may involve making the
throttle a generic module that can be included in any service with minimal code
modification.
