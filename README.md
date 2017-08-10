<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2017, Joyent, Inc.
-->

# manta-muskie: The Manta WebAPI

This repository is part of the Joyent Manta project.  For contribution
guidelines, issues, and general documentation, visit the main
[Manta](http://github.com/joyent/manta) project page.

manta-muskie holds the source code for the Manta WebAPI, otherwise known as
"the front door".  It is analogous to CloudAPI for SDC.  See the restdown
docs for API information, but effectively this is where you go to call
PUT/GET/DEL on your stuff, as well as to submit and control compute jobs.

API documentation is in [docs/index.md](./docs/index.md).  Some design
documentation (possibly quite dated) is in [docs/internal](./docs/internal).
Developer notes are in this README.


## Testing

muskie comes with its own set of unit tests.  You typically test muskie by
starting up a local instance of the server that's configured to point to the
rest of your existing SDC/Manta deployment.  This setup depends on several
prerequisites in your development environment:

1. You should set up a **non-operator** SDC account that will also have access
   to Manta.  The manta-deployment zone includes a tool called add-dev-user that
   can be used to do this.  You can find it at
   /opt/smartdc/manta-deployment/tools/add-dev-user.
   Note: You should not use the "poseidon" user for running the tests.
2. The ssh key that you use to authenticate as this account should be
   passwordless.  It must also be stored locally, with the public key being
   called $HOME/.ssh/id\_rsa.pub.
3. Your SDC and Manta environment variables should point at the SDC and Manta
   instances that you're testing with.  The SDC and Manta variables should refer
   to the same user account, and they should both refer to the ssh key stored in
   $HOME/.ssh/id\_rsa.pub mentioned above.
4. Before running the tests, you must set the `MUSKIE_SALT`, `MUSKIE_KEY`, and
   `MUSKIE_IV` environment variables to the same values being used for the muskie
   instances in your existing Manta installation.  You can find these values in
   SAPI, using:

        sdc-sapi /services?application_uuid="$(sdc-sapi \
            /applications?name=manta | json -H application_uuid)&name=webapi" |
            json -H -a metadata
5. You'll need to create a muskie configuration file that's appropriate for your
   environment.  The easiest way to do this is to copy "etc/config.coal.json" in
   this repo into a new file "config.json".  Then:

   a. Modify all instances of "coal.joyent.us" with the DNS name for your SDC or
      Manta install.  You can replace these DNS names with IP addresses, or you
      can use hostnames and configure /etc/resolv.conf with *both* the SDC and
      Manta resolvers.  Either way, your dev/test zone must be on both the
      "admin" and "manta" networks in order to communicate with both SDC and
      Manta components.

   b. Replace the "salt", "key", and "iv" values in the "authToken" section with
      the corresponding `MUSKIE_` configuration variables described in step 4
      above.

   c. If you would like, replace the "datacenter", "server\_uuid", and
      "zone\_uuid" fields with appropriate values from your setup. If these
      fields are not updated, the metric collection facility will use the
      defaults provided in the file, which may not represent the real values of
      your machine. This step is not required.


In summary, you should make sure these environment variables are set properly:

| **Environment variable** | **Details** |
| ------------------------ | ----------- |
| `MANTA_URL`               | points to port 8080 the instance of muskie that you're testing |
| `MANTA_USER`              | refers to your non-operator user created above |
| `MANTA_KEY_ID`            | refers to a passwordless ssh key in $HOME/.ssh/id\_rsa |
| `MANTA_TLS_INSECURE `     | usually 1 in an environment with self-signed certificates |
| `SDC_URL`                 | points to the SDC deployment that you're using to test |
| `SDC_ACCOUNT`             | same value as `MANTA_USER` |
| `SDC_KEY_ID`              | same value as `MANTA_KEY_ID` |
| `SDC_TESTING`             | analogous to `MANTA_TLS_INSECURE`, but for SDC |
| `MUSKIE_IV`               | from values in SAPI (see above) |
| `MUSKIE_KEY`              | from values in SAPI (see above) |
| `MUSKIE_SALT`             | from values in SAPI (see above) |

On a test system called "emy-10.joyent.us", these may look like this:

    MANTA_URL=http://localhost:8080
    MANTA_USER=dap
    MANTA_KEY_ID=43:7b:f1:98:41:9c:37:90:18:b9:07:92:07:ac:a9:eb
    MANTA_TLS_INSECURE=1
    SDC_URL=https://cloudapi.emy-10.joyent.us
    SDC_ACCOUNT=dap
    SDC_KEY_ID=43:7b:f1:98:41:9c:37:90:18:b9:07:92:07:ac:a9:eb
    SDC_TESTING=1

To run the tests:

1. Run `make` to build muskie.  This will pull down the correct Node executable
   for your platform and your version of muskie and then `npm install` dependent
   modules.
2. Configure your user account for access control by running:

        $ build/node/bin/node test/acsetup.js setup

   This uses the local copy of Node (pulled down as part of the build) to run
   the access-control setup script.  This creates roles and other server-side
   configuration that's used as part of the tests.
3. Start muskie using the configuration file you created above:

        $ build/node/bin/node main.js -f etc/config.json

4. Run `make test`.  (Due to
   [npm issue #4191](https://github.com/npm/npm/issues/4191), this step always
   reinstalls several dependencies.)

If you run into any problems when following this procedure against the latest
version of #master, please let us know.  There are are a couple of things to
check first before reporting problems:

1. If a test fails that has a name like "(fails if MANTA\_USER is operator)",
   then check to see that your `MANTA_USER` is indeed **not** an operator.
2. If a test fails with an InvalidRoleTag error, whose message may say something
   like 'Role tag "muskie\_test\_role\_default" is invalid.', then check that
   you ran the acsetup script described above for the user that you're using.
   (Note that you may see some other muskie\_test\_role in the message.)
3. If a test fails with a message like "Error: MUSKIE\_SALT required", then
   check that you've specified the three `MUSKIE_` environment variables described
   above.
4. If a test fails due to authorization errors (perhaps while completing a job),
   you may have an incorrect muskie configuration. Check that the `MUSKIE_ID`,
   `MUSKIE_KEY` and `MUSKIE_IV` attributes in your config.json match the environment
   variables set for the user running the tests (`$MANTA_USER`).
5. If the "rmdir mpuRoot" and "ls top" tests fail, MPU may not be enabled. If
   you recently upgraded from a pre-MPU muskie version, ensure the line
   '"enableMPU": true' is present in your config.json.

If you're changing anything about the way muskie is deployed, configured, or
started, you should definitely test creating a muskie image and deploying that
into your Manta.  This is always a good idea anyway.

## Metrics

Muskie exposes metrics via [node-artedi](https://github.com/joyent/node-artedi).
See the [design](./docs/internal/design.md) document for more information about
the metrics that are exposed, and how to access them. For development, it is
probably easiest to use `curl` to scrape metrics:

```
$ curl http://localhost:8881/metrics
```

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
name "webapi") uses [Registrar](https://github.com/joyent/registrar/) to
register its instances in internal DNS so that other components can find them.
The general mechanism is [documented in detail in the Registrar
README](https://github.com/joyent/registrar/blob/master/README.md).  There are
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
Muppet.**  (This may explain why [muppet](https://github.com/joyent/muppet) is a
ZooKeeper consumer rather than just a DNS client.)
