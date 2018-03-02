#!/usr/sbin/dtrace -Cs
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/*
 * Reports the following statistics for each muskie process in the zone:
 *  - Average throttle queue depth
 *  - Average in-flight request count
 *  - Average request queueing delays
 *  - Number of requests throttled in the last second
 *  - Number of requests handled in the last second
 *  - Number of requests reaped in the last second
 */


#pragma D option quiet

int latencies[char *];

BEGIN
{
    lines = 0;
}

muskie-throttle*:::throttle_stats
{
    @queued[pid] = avg(arg0);
    @inflight[pid] = avg(arg1);
}

muskie-throttle*:::queue_enter
{
    latencies[copyinstr(arg0)] = timestamp;
}

muskie-throttle*:::queue_leave
/latencies[copyinstr(arg0)]/
{
    latencies[copyinstr(arg0)] = timestamp - latencies[copyinstr(arg0)];
    @latency[pid] = avg(latencies[copyinstr(arg0)] / 1000000);
    latencies[copyinstr(arg0)] = 0;
}

muskie-throttle*:::request_throttled
{
    @throttled[pid] = count();
}

muskie-throttle*:::request_handled
{
    @handled[pid] = count();
}

muskie-throttle*:::request_reaped
{
    @reaped[pid] = count();
}

profile:::tick-1sec
/lines < 1/
{
    lines = 5;
    printf("PID      QDEPTH    INFLIGHT    QDELAY    HANDLED    THROTTLED" +
            "    REAPED\n");
}

profile:::tick-1sec
{
    lines -= 1;
    printa("%-8d %@4u      %@4u        %@4u       %@4u      %@4u        %@4u\n",
            @queued, @inflight, @latency, @handled, @throttled, @reaped);

    /* Clear per-time-interval stats */
    clear(@throttled);
    clear(@handled);
    clear(@reaped);
}
