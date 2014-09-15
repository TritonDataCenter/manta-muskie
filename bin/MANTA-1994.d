#!/usr/sbin/dtrace -s
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

#pragma D option destructive

muskie*:::client_close
{
        this->file = strjoin(
                             strjoin(
                                     strjoin(
                                             strjoin("core.",
                                                     lltostr(walltimestamp / 1000000000)),
                                             "."),
                                     lltostr(pid)),
                             ".ticket=MANTA-1994");

        stop();
        system("gcore -o %s %d", this->file, pid);
        system("prun %d", pid);
        system("mkdir -p /var/tmp/thoth");
        system("mv %s.%d /var/tmp/thoth", this->file, pid);
        boom = 1;
}

muskie*:::socket_timeout
/boom/
{
        this->ts = lltostr(walltimestamp / 1000000000);
        printf("%d(%s): %s\n", pid, this->ts, json(copyinstr(arg0), "id"));
        boom = 0;
}
