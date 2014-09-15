<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

# Abstract

Manta has never had a story for jobs that are `done`.  To date, we've completely
ignored this, and simply leave jobs in the single moray database.  This document
describes a new approach to jobs via Muskie as a whole for end user consumption,
where jobs are archived to Manta as normal objects.

# Why are we doing this?

There are several problems with what we're (not) doing today.  First, and
arguably most importantly, it's highly confusing to users.  Almost everybody at
some point realizes jobs are living forever, and asks how to clean them up.  If
they don't care about cleanup, they care that listing jobs spews a riduculous
amount of text at them, in a non-sorted order (the ids are V4 UUIDs, after all).
Lastly, as a point of reference, the PG database in us-beta (as of 3/7) is 17Gb,
has 3M output records, 2.5M input records, and 17k jobs.  And that's with
basically 3 active users.  Clearly we all can do the math on what real usage of
manta would look like.  We have no choice but to do *something* about this
problem.

# What does the existing Jobs API look like?

Recall that today the Jobs API of Muskie is completely different than the
storage path.  Some of that delta is necessary, other pieces (like listing)
are likely not necessary, just "what we did before".  At any rate we have
this scheme:

```
// Creates a Job, takes a JSON manifest, returns an id
POST /:user/jobs


// Submits input keys, like /mark/stor/foo to the job
POST /:user/jobs/:id/in

// Ends input for a job
POST /:user/jobs/:id/end

// Cancels a job
POST /:user/jobs/:id/cancel

// Retrieves the output keys for the last phase of the job
GET /:user/jobs/:id/out

// Retrieves the list of errors
GET /:user/jobs/:id/err

// Retrieves the original list of inputs
GET /:user/jobs/:id/in

// Returns the list of input keys that failed (that we can determine)
GET /:user/jobs/:id/fail

// Lists all jobs, and allows the user to do some basic filtering
GET /:user/jobs(?state=running)
```

In addition to those APIs, we have `jobs storage` where marlin writes all data
as part of a job, such that it's namespaced, and can be `rm -r`'d. This segment
of the namespace acts exactly like the standard `/:user/stor` APIs, with the
exception that the user cannot write to it - only read and delete.  But for
completeness it is scoped as `/:user/jobs/:id/stor`.

Note in the list above, there are no `DELETE` APIs, which is problematic, at
best.  Also, pagination is barely supported, and only exists on some of the APIs
(namely get job inputs/outputs).

# Ok. What is the new model (in English)?

The new model is one where the API looks *mostly* like the one above. I am
(mostly) only proposing changing the API for `ListJobs`, so it is consistent
with directory listing. But more on that in a bit.  In the new world, for the
"workhorse piece" of the jobs API (which is `/:user/jobs/:id/(in|out|err)`), we
will strongly distringuish between a running job and a done job.  The
content-type and returned format will look the same as it does now (in both
cases), but we will eschew any pretense of pagination, and simply say that a
"live" job is akin to `tail -f`.  Only when a job is `done` can users get the
complete set of keys for those endpoints.

What that means is that when a job is live, users will get the *latest* 1000 (or
whatever limit N is) records for the `in/out/err` firehoses, not a list they can
walk.  Once the job is done, an out of band process will move it into permanent
storage as "flat file manifests".  Users can then retrieve and manage that data
as they would any other object.  Since it will be a contiguous text stream that
also means large output (or error et al) sets will go _a lot_ faster than they
go today.

To make this concrete, let's walk through an example (and again, ignoring
`list`, for the time-being).

```
T0: mmkjob -m wc ==> 8385b3c2-8788-11e2-beea-33746b016f87

T1: mfind /mark/stor/tmp | maddkeys -e 8385b3c2-8788-11e2-beea-33746b016f87

// Returns 10 keys (let's name them 0-9)
T2: mjob -o 8385b3c2-8788-11e2-beea-33746b016f87

// Returns 50 keys (0-49)
T3: mjob -o 8385b3c2-8788-11e2-beea-33746b016f87

// Returns 1000 keys (let's say 101-1001)
T4: mjob -o 8385b3c2-8788-11e2-beea-33746b016f87

// Returns 1000 keys (let's say 828-1828)
T5: mjob -o 8385b3c2-8788-11e2-beea-33746b016f87

T6: job completes

T7: job_archiving moves in/out/err/fail to
    /mark/jobs/8385b3c2-8788-11e2-beea-33746b016f87

// Returns 1828 keys
T8: mjob -o 8385b3c2-8788-11e2-beea-33746b016f87
```

That's basically it.  To the user this is mostly transparent. Obviously when
the job changes state to `done` they get a much bigger result set, but they
can now in code depend on waiting for the job to finish, and simply dumping
the output set with one call, which is much better than all the pagination
garbage.

Now the complex part, `ListJobs`.  What we'd really like to support is the
ability to walk _all_ jobs in exactly the same way we walk directories. However,
recall that directory listings are ordered by name (like the venerable `ls`),
but job names are just UUIDs; sorting on that sucks.  What we really want is to
order jobs in either order of creation or order of completion (I argue creation
so the list is consistent, no matter what).  Also, to be useful, we need to
allow the user to specify filter parameters - notably `?state=running`.  These
two goals bring up (at least a few) problems:

- We have two sources of job data: "stale" jobs, and "live" jobs. The former are
  in manta proper, the latter in marlin's moray.
- We can't blindly reuse the `marker` scheme directories have for pagination (if
  you don't know what that means - you paginate directories by simply passing in
  the last key you saw as a query param, like GET /mark/stor?marker=moo), since
  that depends on ordering by name, not date.

In the interest of not insulting your intelligence, dear reader, I will not
strawman you with false proposals.  The proposal I have is based on on
discussions with dap and trent, both of whom arrived at the same conclusion
independently.

The proposal for supporting `ListJobs` nicely is very simple:  we create a
directory at `mkjob` time and simply stash an `m-` header that indicates the
state of the job.  Running `ListJobs` simply walks down the set of `dirents`,
and returns the "keys" (job UUIDs) that it finds.  The caveat is the ordering
problem - while I think it is highly desirable to look exactly like a directory
listing, it doesn't seem practical or useful, so we will introduce a
"page token" as an HTTP header the user can pick out to call back in with as the
`marker` parameter on the query string (side point: it seems prudent to change
directory listings to the same mechanism for consistency).

The other downside to this approach is that now we have all the problems a
two-phase commit system brings, which is data inconsistency (e.g., suppose we
crash after making the dirent, but before the job is given to marlin).  To work
around this, we will need to make an async zombie cleaner that finds all
"live" job entries, and looks for their absence in marlin.

# The API Details

Without further ado, here's the new proposed scheme in order that a user would
work with the API, with state details:

First, he lists and sees nothing:

```
GET /:user/jobs
```

Because marlin is magical, he creates a job, and gets back a job id:

```
// Create a job
POST /:user/jobs

{
  phases: ...,
  ...
}
```

Now, to sanity check, he lists again:

```
GET /:user/jobs

$jobId
```

Here we will no longer return all the job details, as we do today; the list will
simply be job names.  Users would need to `GetJob` to echo back details:


```
GET /:user/jobs/:id

{...}
```

However, to facilitate seeing `live` jobs, the user can run:

```
GET /:user/jobs?state=running

...
```

And get back a list of ids.  Note that as of right now, my plan is to say "listing
live jobs" gives you the most recent 1000.  If you have more than 1000 live jobs,
tough luck.  Go walk the full job listing if you're in that state.

Side point, if you've made it here, dear reader, congratulations.  The quiz word
is "Snuffleupagus."  Now the user feeds some keys in, as before (let's say 2001
keys), and then closes input:

```
POST /:user/jobs/:id/in

...

POST /:user/jobs/:id/end
```

Now that the user has made a job and fed input keys, as described above, he
`tail -f`'s the job (side point: websockets with push would be way cooler, but
no way is that v1):

```
GET /:user/jobs/:id

327 keys
```

And so on. Finally, when the job has moved to the `done` state, the full
manifest(s) are available.

# Implementation Details

A few sucky notes:

- It's problematic to run > 1 puller, because we don't want them stomping on
  each other.  I'm planning to do something like "all pullers know how many
  total pullers there are, and they hash `workerId` to the number of pullers
  as buckets."  Seems like a cheap easy way to avoid conflicts, and we can still
  do something like "also pull anything that is older than N minutes".
- To make `GetJob` look like it does today, *and* be a directory, it's going to
  be pretty gross.  Basically, I'm going to serialize the job manifest (the blob
  of JSON that describes phases et al) into an HTTP header as metadata, and just
  stash it that way.  It doesn't *really* matter, because it's really all the
  same space consumed in the DB, it's just not obvious.

# One more thing

Dave points out that Marlin already has telemetry data for task timing stored in
moray.  I would *love* to integrate that as part of this into job pulling, such
that when a job is done a `/:user/jobs/:id/metrics` pops into existence with all
timing data, etc.  I don't understand the details of what he has, so I'm leaving
it out of scope for now, other than to point out, I think we want to do that.

Also, we could use that same data for our own operational metrics, but we would
need to ensure that the data retention is set to be owned by us, not the user -
we could simply make links to the final data like `/:user/jobs/:id/metrics` ->
`/poseidon/stor/job_metrics/...`.  That would obviously be stripped of any
internal timings, and only be the end user visible metrics.  My conjecture for
operational timings is that we want that captured in logs, so it's moved through
the system the same way other temetry data is (i.e., for manowar).
