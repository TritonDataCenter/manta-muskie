<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

# Access Control User Guide

With the introduction of access control for the Joyent Cloud, customers can
create users under their accounts and maintain fine-grained control over
resources.

Access control features only apply to users created under accounts. Making
requests as an account grants full access to all resources under the account.

## Security and Access Control

There are a few security related concepts that are referenced in this guide.

* Identity - a uniquely identifiable actor. An identity can be a human, or
  machine, or even a process. Identities in this guide will generally be humans.
* Authentication - verification of an identity from provided credentials, which
  could be a username and password combination or SSH keys.
* Authorization - the actual granting or denying of access to a resource for an
  identity.

<!-- * Audit - the ability to look back and confirm historical events -->

Access control allows you to control access to your data by specifying rules
that describe who can access what and under what conditions. Rules can be
broken down to four logical components.

* Principal - the "subject", or the identity the rule applies to e.g. Bob
  the Contractor. Principals in the Joyent Cloud are users.
* Action - the "verb", or the action a user takes e.g., PutObject, CreateJob
* Resource - the "object", or target of the action e.g., machine, Manta object
* Context - any extra constraints e.g. "between 08:00-17:00", "from eu-ams1"

Rules for access control for Joyent are written with the [Aperture][aperture]
access control language. With Aperture, you can define rules with each of the
components above in a human-readable way.

For a request to be authorized, the principal, action, resource and context for
the request are evaluated against any relevant rules. If any of the rules grant
access, the request is authorized. The Aperture evaluation engine is used to
authorize requests.

> ###### Example: Rule Components
> A company has traffic logs from their website stored in Manta. They want
> to give access to read their traffic logs to a contractor specializing in
> search engine optimization.
> The principal is the contractor. The action is read. The resources are the
> traffic logs. The context includes the time of the request and the source
> IP address, and any other context that is available.

Access control lists (ACLs) are one form of access control where rules are
stored with resources (like in a UNIX file system). ACLs describe which
identities can access the resource they are attached to.

> ###### Example: ACLs
> In the same scenario, the company can use ACLs by specifying rules on the
> traffic logs that say the contractor can access them.

Our model is based on a ACLs plus roles for easier management. Here's how it
works.

## Quick Start

Roles are a group of users that share a set of rules. For ACLs, instead a list
of users that can access the resource, we use *role tags*, or a list
of roles that can access the resource.

To get started with access control, first create a user. Users are separate
identities under an account with their own credentials.

    TODO cloudapi create user "bob"

Then, create a role and add Bob to the role.

    TODO cloudapi create role "contractor" with (default) member "bob"

Next, add some rules to the role. These rules will be evaluated whenever a
user makes a request and the resource has the role in its list of role tags.

    TODO cloudapi create policy "read" with rule Can getobject
    TODO cloudapi add policy "read" to role "contractor"

Bob can now take the action `getobject` on any resource that is tagged with the
`read` role tag. Add a role tag to a Manta object.

    mchmod +contractor ~~/stor/traffic-logs.txt

Bob can now read `traffic-logs.txt`.

    MANTA_USER=bob MANTA_ACCOUNT=example mget ~~/stor/traffic-logs.txt

For a more detailed example, see the "Making a Request" example below.

## Users and Roles

### User Management

The basic building blocks of access control in the Joyent Cloud are rules,
policies, users and roles.

* **Rules** are specified using individual Aperture sentences. They contain
  actions and conditions (`Can getobject if sourceip = 10.0.0.0/24`). See below
  for more details about Aperture.
* **Policies** are a list of rules.
* **Users** are separate identities under an account with their own
  credentials.
* **Roles** consist of a list of users and a list of policies. Each of the
  users in a role's member list may assume the role, and rules from policies on
  that role will be used to authorize requests by those users.
  Roles also have a third list that contain users for which the role is a
  *default role*. More on default roles below.

<!-- -->

    Account
    |
    +---- User
    |     |
    |     +-- SSH Keys
    |
    +---- Role
    |     |
    |     +-- Users
    |     +-- Users (default)
    |     +-- Policies
    |
    +---- Policy
          |
          +-- Rules

> ###### Example: Adding a user
>
>     $ TODO cloudapi add a user

<!-- -->

> ###### Example: Adding a role
>
>     $ TODO cloudapi add a role

<!-- -->

> ###### Example: Adding a policy
>
>     $ TODO cloudapi add a policy
>     $ TODO cloudapi add a rule to policy

<!-- -->

> ###### Example: Adding a policy to a role
>
>     $ TODO cloudapi add a policy to role

<!-- -->

> ###### Example: Add a user to a role
>
>     $ TODO cloudapi add user to role

There is one system-reserved role, the administrator role, and one
system-reserved user, the anonymous user.

#### The Administrator Role

If you create a role named "administrator", that role is recognized as the
special administrator role. The administrator role is similar to "sudo"
in UNIX: any user that assumes the administrator role can do anything the
account owner can do.

As in UNIX, it is recommended that you avoid authenticating as the account
owner, and instead create a user with administrator access and assume the
administrator role with that user for any further operations that need
privileged access.

Note: You cannot attach policies to the "administrator" role (since all requests
to your account will be authorized).

#### The Anonymous User

If you want to allow public access to objects, you can create a user with the
name "anonymous". The anonymous user will be used as the user making the
request for unauthenticated requests, or if authorization fails for an
authenticated user.

Using the anonymous user, you can control the conditions under which a resource
is publicly accessible by making the anonymous user a member of roles, just
like any other user.

Note: Everything under ~~/public is accessible to anyone, regardless of role
tags or other context.

> ###### Example: Allowing public reads on a Manta object
>
> First, create a user named "anonymous".
>
>       $ TODO cloudapi create user "anonymous"
>
> Then, create a role for the anonymous user. You can assign the anonymous user
> to an existing role, but be aware that that would allow public access to
> objects already tagged with that role.
>
>       $ TODO cloudapi create role "public-read" with "anonymous" member
>
> Attach a policy that allows GETs.
>
>       $ TODO cloudapi create `Can getobject` policy called "read"
>       $ TODO cloudapi add "read" policy to "public-read"
>
> Finally, tag the object with the role.
>
>       $ mchmod +public-read ~~/stor/shared.txt


#### Default Roles

Users may choose which roles they want to assume and be active for a request.
If no roles are provided, then all roles that have the user in its default
users list will be active. If a user tries to assume a role that he or she
doesn't have access to, the service will reject the request.

The set of all roles that the user can assume (default or otherwise) is
referred to as his *limit set* of roles.

The administrator role is one of the primary use cases for default roles. By
leaving users out of the default users list on the administrator role, users
can make requests normally without worrying about accidentally doing
potentially dangerous operations (like `mrm -rf ~~/stor`), but still have the
ability to assume the administrator role if needed.

> ###### Example: The administrator role and default roles
>
>     $ mrm ~~/stor/do-not-delete.txt
>     mrm: AuthorizationFailedError: example/fred is not allowed to access /example/stor/do-not-delete.txt
>     $ mrm --roles=administrator ~~/stor/do-not-delete.txt
>     $

### Resource Management

Role tags are metadata attached to resources that identify which roles apply to
the resource. So, for successful authorization, the user must have an active
role for that request that matches a role tag on the resource *and* that
contains a rule that, when evaluated, results in a successful authorization. By
tagging a resource with a role, you are essentially filling in the principal
and resource parts of a rule: the *principal* is anyone that has access to the
role on the *resource* that you tagged.

Additionally, whenever a resource is created, all of the active roles from the
request are automatically applied to the resource as role tags. You can
override this behavior in node-manta using the `--role-tag` option on `mput` or
`mmkdir`. You can modify the role tags on an existing resource in Manta with
the `mchmod` command.

Note that you can tag resources with roles that you do not have access to, and
this can result in you locking yourself out of access to an object.

> ###### Example: mchmod
>
> Add `role1` to `file.txt`
>
>     mchmod +role1 ~~/stor/file.txt
>
> Add `role2` and remove all other roles on `directory`
>
>     mchmod =role2 ~~/stor/directory

### Making a Request

When making a request as a user, a list of rules along with the request context
is evaluated using Aperture. The list of rules comes from a set of roles that
are relevant to this request. Relevant roles are the roles that

* are part of the user's limit set of roles, and
* are active for the request, whether by default or explicitly specified, and
* are in the list of role tags on the resource.

For a rule to evaluate to to true, the action and conditions from the rule have
to match the action and conditions from the request.

If *any* of the rules from the relevant roles evaluate to true (an OR across
rules), the request is authorized.

### Access Control in CloudAPI

Resources in CloudAPI are each machine as well as a top level `/machines`
resource that you can use to control VM creation and listing.

TODO expand

### Access Control in Manta

#### Objects and Directories

Objects and directories are the resources in Manta, and they can both be tagged
with roles.

With respect to authorization actions, operations in Manta are classified as a
put, get or delete on an object or directory (e.g. `putdirectory`), plus
`putlink`, which also requires `getobject` access on the source of the
snaplink.

When you create a new object (`putobject`, `putdirectory`, `putlink`), the
parent directory is the resource, so role tags are pulled from the parent
directory. So, in order to allow object creation, there must be a role that
allows `putobject` access on the directory.

#### Manta Jobs

Job creation and listing access is checked by getting role tags from your
`~~/jobs` directory.

When you create a job, all the context from the job creation request is saved
and used when making Manta requests from within the job. Roles that were active
when the job was created will be used as the active roles for requests in a
job. The same roles will be used to check for `getobject` access for job inputs.

This also means that objects and directories created during a job are tagged
with the same roles that were active at job creation. This includes objects and
directories under `~~/jobs/<uuid>/stor` like intermediate objects (TODO
MANTA-2173).

An additional piece of context `fromjob` (boolean) is also added to those
requests.

## Examples

### Making a Request

George helps with customer support issues. He has access to a role "support"
that lets him read and write support tickets.

Here are the roles:

Roles:

| Name | Users | Default Users | Policies |
| --- | --- | --- | --- |
| **support** | george | george | read, write |

Policies:

| Name | Rules |
| --- | --- |
| **read** | `Can getobject ` |
| **write** | `Can putobject` |

    TODO cloudapi create user "george"
    TODO cloudapi create policy "read"
    TODO cloudapi create policy "write"
    TODO cloudapi create role "support" with policies "read" and "write" + george

And here are the objects in Manta, with role tags in brackets:

    ~~/stor
    └── support-tickets [ support ]
        └── issue1.txt [ support ]

George is contacted by a customer about an issue and creates a new ticket.
When he makes his request, he'll first identify himself as George and presents
his SSH key ID along with a signed header. After Manta authenticates George,
the next step is authorization.

Since George wants to create a new object, the action he is taking is
`putobject`, and the resource is the parent directory ("support-tickets" in
this case).

George didn't include in his request any roles to assume, so his default roles
will be active. In this case, that's the "support" role.

Since George has access to the "support" role, and the "support" role is
active on this request, and the "support" role is present on
`~~/stor/support-tickets`, the rules from the "support" role are evaluated.

The first rule `Can getobject` evaluates to false because the action George
is taking is `putobject`, not `getobject`. The second rule evaluates to
true. Since at least one rule evaluated to true, access is granted.

Since "support" was active for that request, the new object is automatically
tagged with "support".

    ~~/stor
    └── support-tickets [ support ]
        ├── issue1.txt [ support ]
        └── issue2.txt [ support ]

### Separating Organizations

Using roles makes it easy to keep data separate between groups of employees.

Roles:

| Name | Users | Default Users | Policies |
| --- | --- | --- | --- |
| **engineer** | fred | fred | read, write |
| **support** | george | george | read, write |
| **hr** | lennie | lennie | read, write |
| **sales** | carl | carl | read, write |

Policies:

| Name | Rules |
| --- | --- |
| **read** | `Can getobject and getdirectory` |
| **write** | `Can putobject, putdirectory, and putlink` |

You can give each organization a directory of their own by tagging a directory
with the respective role. Then, each organization has its own directory, and
can't access data from any other organization.

    ~~/stor
    ├── code [ engineer ]
    ├── personnel [ hr ]
    ├── leads [ sales ]
    └── support-tickets [ support ]

Whenever George, a Support Guy, needs to track a support issue, he can create a
ticket under `support-tickets`, which will automatically be tagged with the
`support` role so all other Support People can access it. George will also not
be able to access anything under the directories belonging to Engineering,
HR or Sales.

### Sharing Access

George the Support Guy determines that the customer's issue is something an
engineer needs to fix. He wants to share the details of the issue with an
engineer, Fred, who often helps with support issues.

Roles:

| Name | Users | Default Users | Policies |
| --- | --- | --- | --- |
| **engineer** | fred | fred | read, write |
| **support** | george | george | read, write |
| **support-helper** | fred | | read |

Policies:

| Name | Rules |
| --- | --- |
| **read** | `Can getobject` |
| **write** | `Can putobject` |

    ~~/stor
    └── support-tickets [ support ]
        ├── issue1.txt [ support ]
        └── issue2.txt [ support ]

If Fred tries to access issue2.txt right now, he'll be denied access:

    $ mget ~~/stor/support-tickets/issue2.txt
    mget: AuthorizationFailedError: example/fred is not allowed to access /example/stor/support-tickets/issue2.txt

George can use `mchmod` to allow Fred to read the object.

    mchmod +support-helper ~~/stor/support-tickets/issue2.txt

<!-- -->

    ~~/stor
    └── support-tickets [ support ]
        ├── issue1.txt [ support ]
        └── issue2.txt [ support, support-helper ]

Since "support-helper" is not one of Fred's default roles, he needs to specify
that he would like to assume the role in order to read issue2.txt

    $ mget -q --roles=support-helper support-helper ~~/stor/support-tickets/issue2.txt
    Bugs something problem something broken something something.

### Producers and Consumers

Lennie the HR Guy wants to set up a suggestion box, where anybody can write a
suggestion and upload it to a directory, but only HR can read the suggestions.

Roles:

| Name | Users | Default Users | Policies |
| --- | --- | --- | --- |
| **hr** | lennie | lennie | read, write, list |
| **public-create** | anonymous | anonymous | create |

Policies:

| Name | Rules |
| --- | --- |
| **read** | `Can getobject and getdirectory` |
| **create** | `Can putobject` |
| **write** | `Can putobject` |

    ~~/stor
    └── suggestionBox [ hr, public-write ]

Then, whenever somebody wants to submit a suggestion, he or she can write it in
the directory, then use `mchmod` to allow `hr` access.

    $ mput -f more-donuts.txt ~~/stor/suggestionBox/more-donuts.txt
    $ mchmod +hr ~~/stor/suggestionBox/more-donuts.txt

Or, to combine the two steps:

    $ mput -f more-donuts.txt --role-tags=public-write,hr ~~/stor/suggestionBox/more-donuts.txt

<!-- -->

    ~~/stor
    └── suggestionBox [ hr, public-write ]
        └── more-donuts.txt [ hr, public-write ]

### Using Context

##### Preventing Overwrites

Giving `putobject` access allows users to overwrite existing Manta objects or
metadata. To prevent this, you can check for the `overwrite` context in the
rule.

`Can putobject if overwrite = false`

##### Limiting Access to Certain IP Addresses

To limit access to only IP addresses belonging to your company's network, use
the `sourceip` context in your rules.

`Can getobject and getdirectory if sourceip = 1.2.3.0/24 or sourceip = 3.2.1.0/24`

<!-- TODO cross account links -->

## Aperture

The rules you write for access control are in a human-readable language that
are parsed and evaluated by [Aperture][aperture].

Aperture is a general purpose access control language, and valid Aperture rules
can contain all the components of a rule, but when writing rules for the Joyent
Cloud, you should only specify the "action" and "conditions" parts of the rule.
The "principal" and "resource" parts are checked by the user having access to
the roles specified in the role tags on the resource, instead of by the rules
themselves. See the Resource Management section above for more details.

Some examples of aperture rules:

* `Can getobject if sourceip = 10.0.0.0/32`
* `Can putobject if day in (Monday, Tuesday, Wednesday, Thursday, Friday)`
* `Can createjob`

For more information see <https://github.com/joyent/node-aperture>

Note: Aperture as a language supports explicit deny as the effect of a rule.
However, explicit deny is not supported in Joyent's access control system.

# Appendix

## Actions

#### CloudAPI Actions

| action | related operations |
| --- | --- |
| ... | ... |


#### Manta Actions

| action | related operations |
| --- | --- |
| getobject | read object, get archived job stats |
| getdirectory | list directories |
| putobject | create objects, overwrite objects, update object metadata |
| putdirectory | create directories, update directory metadata |
| putlink | create snaplinks\* |
| deleteobject | delete objects |
| deletedirectory | delete (empty) directories |
| createjob | create jobs |
| managejob | add input keys to jobs, end job input, cancel jobs |
| listjobs | list jobs |
| getjob | get live job status, errors, inputs, and outputs |

\* You must also have `getobject` access on the source.

## Context for Rules

#### General

A list of context available to include in rules

| name | [type][types] | description |
| --- | --- | --- |
| activeRoles | array | list of active roles |
| date | date | date of the request |
| day | day | day of the week the request is made (Monday, Tuesday, ...) |
| sourceip | ip | source ip address of the caller |
| time | time | time of day the request was made |
| user-agent | string | user agent of the caller |
| region | string | manta region to which the request was made |
| ... | ... | ... |

#### CloudAPI

Context specific to CloudAPI requests:

| name | [type][types] | description |
| --- | --- | --- |
| ... | ... | ... |

#### Manta

Context specific to Manta requests:

| name | [type][types] | description |
| --- | --- | --- |
| fromjob | boolean | true iff the request was made from within a Manta job |
| overwrite | boolean | true iff a request is overwriting an existing object or metadata |
| parentdirectory | string | full path of the parent directory |
| ... | ... | ... |

[aperture]: https://github.com/joyent/node-aperture
[types]: https://github.com/joyent/node-aperture#types
