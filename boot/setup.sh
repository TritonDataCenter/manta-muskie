#!/bin/bash
# -*- mode: shell-script; fill-column: 80; -*-
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

set -o xtrace

SOURCE="${BASH_SOURCE[0]}"
if [[ -h $SOURCE ]]; then
    SOURCE="$(readlink "$SOURCE")"
fi
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
PROFILE=/root/.bashrc
SVC_ROOT=/opt/smartdc/muskie

source ${DIR}/scripts/util.sh
source ${DIR}/scripts/services.sh


export PATH=$SVC_ROOT/build/node/bin:$SVC_ROOT/node_modules/.bin:/opt/local/bin:/usr/sbin:/usr/bin:$PATH


function wait_for_resolv_conf {
    local attempt=0
    local isok=0
    local num_ns

    while [[ $attempt -lt 30 ]]
    do
        num_ns=$(grep nameserver /etc/resolv.conf | wc -l)
        if [ $num_ns -gt 1 ]
        then
		    isok=1
		    break
        fi
	    let attempt=attempt+1
	    sleep 1
    done
    [[ $isok -eq 1 ]] || fatal "manatee is not up"
}


function manta_setup_muskie {
    local num_instances=1
    local size=`json -f ${METADATA} SIZE`
    if [ "$size" = "lab" ]
    then
        num_instances=4
    elif [ "$size" = "production" ]
    then
	num_instances=16
    fi

    #Build the list of ports.  That'll be used for everything else.
    local ports
    local insecure_ports
    for (( i=1; i<=$num_instances; i++ )); do
        ports[$i]=`expr 8080 + $i`
        insecure_ports[$i]=`expr 9080 + $i`
    done

    #To preserve whitespace in echo commands...
    IFS='%'

    #haproxy
    for port in "${ports[@]}"; do
        hainstances="$hainstances        server muskie-$port 127.0.0.1:$port check inter 10s slowstart 10s error-limit 3 on-error mark-down\n"
    done
    for insecure_port in "${insecure_ports[@]}"; do
        hainstances_insecure="$hainstances_insecure        server muskie-$insecure_port 127.0.0.1:$insecure_port check inter 10s slowstart 10s error-limit 3 on-error mark-down\n"
    done

    sed -e "s#@@MUSKIE_INSTANCES@@#$hainstances#g" \
	-e "s#@@MUSKIE_INSECURE_INSTANCES@@#$hainstances_insecure#g" \
        $SVC_ROOT/etc/haproxy.cfg.in > $SVC_ROOT/etc/haproxy.cfg || \
        fatal "could not process $src to $dest"

    svccfg import $SVC_ROOT/smf/manifests/haproxy.xml || \
	fatal "unable to import haproxy"

    #muskie instances
    local muskie_xml_in=$SVC_ROOT/smf/manifests/muskie.xml.in
    for (( i=1; i<=$num_instances; i++ )); do
        local muskie_instance="muskie-${ports[i]}"
        local muskie_xml_out=$SVC_ROOT/smf/manifests/muskie-${ports[i]}.xml
        sed -e "s#@@MUSKIE_PORT@@#${ports[i]}#g" \
	    -e "s#@@MUSKIE_INSECURE_PORT@@#${insecure_ports[i]}#g" \
            -e "s#@@MUSKIE_INSTANCE_NAME@@#$muskie_instance#g" \
            $muskie_xml_in  > $muskie_xml_out || \
            fatal "could not process $muskie_xml_in to $muskie_xml_out"

        svccfg import $muskie_xml_out || \
            fatal "unable to import $muskie_instance: $muskie_xml_out"
        svcadm enable "$muskie_instance" || \
            fatal "unable to start $muskie_instance"
        sleep 1
    done

    # Setup haproxy after the muskie's are kicked up
    svcadm enable "manta/haproxy" || fatal "unable to start haproxy"

    unset IFS

    # add manatee metadata backup cron

    local crontab=/tmp/.manta_webapi_cron
    crontab -l > $crontab

    echo "30 * * * * /opt/smartdc/muskie/bin/backup_pg_dumps.sh >> /var/log/backup_pg_dump.log 2>&1" >> $crontab
    [[ $? -eq 0 ]] || fatal "Unable to write to $crontab"
    crontab $crontab
    [[ $? -eq 0 ]] || fatal "Unable import crons"

    #.bashrc
    echo 'function req() { grep "$@" /var/log/muskie.log | bunyan ;}' \
        >>/root/.bashrc
}


function manta_setup_muskie_rsyslogd {
    #rsyslog was already set up by common setup- this will overwrite the
    # config and restart since we want muskie to log locally.
    local domain_name=$(json -f ${METADATA} domain_name)
    [[ $? -eq 0 ]] || fatal "Unable to domain name from metadata"

    mkdir -p /var/tmp/rsyslog/work
    chmod 777 /var/tmp/rsyslog/work

    cat > /etc/rsyslog.conf <<"HERE"
$MaxMessageSize 64k

$ModLoad immark
$ModLoad imsolaris
$ModLoad imudp


$template bunyan,"%msg:R,ERE,1,FIELD:(\{.*\})--end%\n"

*.err;kern.notice;auth.notice			/dev/sysmsg
*.err;kern.debug;daemon.notice;mail.crit	/var/adm/messages

*.alert;kern.err;daemon.err			operator
*.alert						root

*.emerg						*

mail.debug					/var/log/syslog

auth.info					/var/log/auth.log
mail.info					/var/log/postfix.log

$WorkDirectory /var/tmp/rsyslog/work
$ActionQueueType Direct
$ActionQueueFileName mantafwd
$ActionResumeRetryCount -1
$ActionQueueSaveOnShutdown on

HERE

        cat >> /etc/rsyslog.conf <<HERE

# Support node bunyan logs going to local0 and forwarding
# only as logs are already captured via SMF
local0.* /var/log/muskie.log;bunyan

HERE

        cat >> /etc/rsyslog.conf <<"HERE"
$UDPServerAddress 127.0.0.1
$UDPServerRun 514

HERE

    svcadm restart system-log
    [[ $? -eq 0 ]] || fatal "Unable to restart rsyslog"

    #log pulling
    manta_add_logadm_entry "muskie" "/var/log" "exact"
}



# Mainline

echo "Running common setup scripts"
manta_common_presetup

echo "Adding local manifest directories"
manta_add_manifest_dir "/opt/smartdc/muskie"

manta_common_setup "muskie"

manta_ensure_zk

echo "Setting up muskie"

# MANTA-1827
# Sometimes muskies come up before DNS resolvers are in /etc/resolv.conf
wait_for_resolv_conf
manta_setup_muskie
manta_setup_muskie_rsyslogd

manta_common_setup_end

#Setup the mlocate alias
echo "alias=mlocate'/opt/smartdc/muskie/bin/mlocate -f /opt/smartdc/muskie/etc/config.json'" >> $PROFILE

exit 0
