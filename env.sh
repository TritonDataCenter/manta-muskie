# -*- mode: shell-script -*-
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

export PATH=$PWD/build/node/bin:$PWD/node_modules/.bin:$PATH

export MANTA_URL=http://127.0.0.1:8080
export MANTA_USER=$USER
export MUSKIE_DATA_TIMEOUT=4000

alias server='node main.js -f ./etc/config.coal.json -v 2>&1 | bunyan'
alias npm='node `which npm`'
