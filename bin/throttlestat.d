#!/usr/sbin/dtrace -Cs
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

#pragma D option quiet

int latencies[char *];

muskie-throttle*:::queue_enter
{
	latencies[copyinstr(arg0)] = timestamp;
}

muskie-throttle*:::queue_leave
/latencies[copyinstr(arg0)]/
{
	latencies[copyinstr(arg0)] = timestamp - latencies[copyinstr(arg0)];
	@avg_latency[pid] = avg(latencies[copyinstr(arg0)] / 1000000);
	latencies[copyinstr(arg0)] = 0;
}

muskie-throttle*:::request_throttled
{
	@throttled[pid] = count();
}

muskie-throttle*:::request_handled
{
	@qlen[pid] = max(arg1);
	@qrunning[pid] = max(arg0);
}

profile:::tick-1sec
/lines < 1/
{
	printf("THROTTLED-PER-SEC | AVG-LATENCY-MS | MAX-QLEN | MAX-RUNNING\n");
	printf("------------------+----------------+----------+------------\n");
	lines = 5;
}


profile:::tick-1sec
/lines > 0/
{
	lines -= 1;
	printa("%@4u              %@4u            %@4u       %@4u\n", @throttled,
            @avg_latency, @qlen, @qrunning);

	clear(@throttled);
	clear(@avg_latency);
	clear(@qlen);
	clear(@qrunning);
}
