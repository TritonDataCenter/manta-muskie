<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

# manta-muskie: The Manta WebAPI

This repository is part of the Joyent Manta project.  For contribution
guidelines, issues, and general documentation, visit the main
[Manta](http://github.com/joyent/manta) project page.

manta-muskie holds the source code for the Manta WebAPI, otherwise known as
"the front door".  It is analogous to CloudAPI for SDC.  See the restdown
docs for API information, but effectively this is where you go to call
PUT/GET/DEL on your stuff.

API, design and developer documentation are in the `docs` directory as
[restdown](https://github.com/trentm/restdown) files.

# Testing

Use `make test` to run the tests.

For the access control tests in test/ac.test.js, some setup is required to add
the test roles and sub-users. Since adding roles and sub-users is done through
CloudAPI, make sure your environment is set up to talk to CloudAPI correctly
(e.g. you have SDC\_\* set properly). Run `node test/acsetup.js setup` to add
them, and `node test/acsetup.js teardown` to clean up the test roles and users,
if desired.

A few tests use token-based authentication that require you to specify the AES
salt, key, and IV installed in Manta in the environment variables `MUSKIE_SALT`,
`MUSKIE_KEY`, and `MUSKIE_IV`, respectively.
