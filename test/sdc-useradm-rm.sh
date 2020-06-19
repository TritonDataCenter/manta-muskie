#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2020 Joyent, Inc.
#
#
# Delete a UDFS account (and all UFDS nodes under it).
# This is what one might want for 'sdc-useradm rm ACCOUNT', but it is
# dangerous and would require more care to add there.
#

if [[ -n "$TRACE" ]]; then
    export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
    set -o xtrace
fi
set -o errexit
set -o pipefail


function fatal
{
    echo "$0: fatal error: $*"
    exit 1
}




#---- mainline

[[ "$(zonename)" == "global" ]] || fatal "not running on a SmartOS global zone"

ACCOUNT_OR_LOGIN=$1
[[ -n "$ACCOUNT_OR_LOGIN" ]] || fatal "missing ACCOUNT/LOGIN arg"

account_json=$(sdc-useradm get "$ACCOUNT_OR_LOGIN")
account_uuid=$(echo "$account_json" | json uuid)
account_login=$(echo "$account_json" | json login)

if [[ "$account_login" == "admin" || "$account_login" == "poseidon" ]]; then
    fatal "nope"
fi

dns=$(sdc-ufds s -b "uuid=$account_uuid, ou=users, o=smartdc" "objectclass=*" \
    | json -ga dn.length dn \
    | sort -k1 -n -r \
    | sed -E 's/^[0-9]+ //')

echo "This will delete the following from UFDS:"
echo "$dns" | while read dn; do
    echo "    $dn"
done
echo ""
if [[ -z "$I_REALLY_WANT_TO_SDC_USERADM_RM" ]]; then
    echo -n "Press <Enter> to continue, Ctrl+C to abort."
    read
fi

echo "$dns" | while read dn; do
    echo "sdc-ufds rm '$dn'"
    sdc-ufds rm "$dn"
    sleep 1
done
