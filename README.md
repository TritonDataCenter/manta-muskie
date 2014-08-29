<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

# Muskie, the Manta WebAPI

Repository: <git@git.joyent.com:muskie.git>
Browsing: <https://mo.joyent.com/muskie>
Who: Mark Cavage
Docs: <https://mo.joyent.com/docs/muskie>
Tickets/bugs: <https://devhub.joyent.com/jira/browse/MANTA>

# Overview

This repo holds the source code for the Manta WebAPI, otherwise known as
"the front door".  It is analogous to CloudAPI for SDC.  See the restdown
docs for API information, but effectively this is where you go to call
PUT/GET/DEL on your stuff.

Muskie is dependent on just about everything else in the Mantaverse, so
to get one of these running, you first need a build out of all other things
Manta: binder, manatee, moray, mako and mahi.

For the complete REST API:  <https://mo.joyent.com/docs/muskie>

# Repository

This repo follows JEG: <https://mo.joyent.com/docs/eng>, so go visit that
to see how this repo is structured, if you're not already familiar.

# Development

In general, it is easiest to just run a local muskie pointed at an existing
manta in the lab or coal.  To do, that see <https://mo.joyent.com/docs/manta>.
Then hack up a local config file (some examples are already checked in), source
env.sh, and run the server.

# Testing

Just run:

    make prepush
