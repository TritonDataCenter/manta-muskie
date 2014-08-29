#!/usr/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

echo ""   # blank line in log file helps scroll btwn instances
# pull in the manta vars
source /root/.bashrc
export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace
PATH=/opt/smartdc/muskie/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:/usr/sbin:/usr/bin:/opt/smartdc/muskie/node_modules/.bin/

# grab the current hour's backup and put it in /var/tmp/manatee_backups/

year=$(date -u +%Y)
month=$(date -u +%m)
day=$(date -u +%d)
hour=$(date -u +%H)

LOCAL_BACKUP_DIR=/var/tmp/manatee_backups/$year/$month/$day/$hour
mkdir -p $LOCAL_BACKUP_DIR

SHARD_PATH=/poseidon/stor/manatee_backups

for shard in $(mls $SHARD_PATH)
do
    dump=$SHARD_PATH/$shard/$year/$month/$day/$hour/$(mls $SHARD_PATH/$shard/$year/$month/$day/$hour/ | grep moray-)
    mkdir -p $LOCAL_BACKUP_DIR/$shard
    /opt/smartdc/muskie/bin/mlocate -f /opt/smartdc/muskie/etc/config.json $dump > $LOCAL_BACKUP_DIR/$shard/moray_dump.json
done
