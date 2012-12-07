============================
Disaster Recovery Overview
============================



Design
============

The following discusses a disaster recovery implementation. A complete
geographic replication solution could be implemented later on top of
it, but will require some more design.

* Primary, secondary clusters

A secondary cluster follows a primary cluster. It is intended that
clients will only be accessing the primary cluster. Read only access
to the secondary will be possible, however, data in the secondary may
be outdated. It is possible to switch primary and secondary settings
of a cluster.

The idea is to have a primary cluster, and one or more secondary
clusters following it. Updates will be logged per bucket on the
primary, and list of modified buckets will also be logged. Secondary
clusters will poll list of modified buckets, and will then retrieve
the changes per bucket. Changes will be applied on the secondary.

As stated, this is a disaster recovery solution, and comes to provide
safety net for complete data loss. The solution does not provide a
complete data loss protection, as latest data that has not been
transferred to the secondaries may be lost.


Primary
-------

Bucket index log
^^^^^^^^^^^^^^^^

The 'bucket index log' will keep track of modifications made in the bucket (objects uploaded, deleted, modified).

The following modifications will be made to the bucket index:

* bucket index version

The bucket index will keep an index version that will increase monotonically.

* log every modify operation

The bucket index will now log every modify operation. An additional
index version entry will be added to each object entry in the bucket
index.

* list objects objclass operation returns more info

list objects objclass operation will also return last index version,
as well as the version of each object (the bucket index version when
it was created). This will allow radosgw to retrieve the entire list
of objects in a bucket in parts, and then retrieve all the changes
that happened since starting the operation. It is required so that we
could do a full sync of the bucket index.

* operation to retrieve bucket index log entries

A new objclass operation will retrieve bucket log index entries. It
will get a starting event number, max entries.
When requested with a

*  operation to trim bucket index log entries

A new objclass operation to remove bucket log index entries. It will
get a starting event number (0 - start from the beginning), last event
number. It will not be able to remove more than predefined number of
entries.


Updated Buckets Log
^^^^^^^^^^^^^^^^^^^

A log that contains the list of modified buckets within a specific
period.  Log info may be spread across multiple objects. This will be
done similarly to what we did with the usage info, and with the
garbage collection. Each bucket's data will go to a specific log
object (by hashing bucket name, modulo number of objects).
The log will use omap to index entries by timestamp, and by a log id (monotonically increasing) and
will be implemented as an objclass.


* log resolution

We'll define a log resolution period. In order to avoid sending extra write for updating this log
for every modifications, we'll define a time length for which a
log entry is valid. Any update that completes within that will only be reported once in the log.

A bucket modification operation will not be allowed to complete before
a log entry (with the bucket name) was appended to the bucket
operations log within the past ttl (the cycle in which the operation
completes). That means that the first write/modification to that
bucket will have to send an append request. All bucket modification
operations that happen before its completion (and within the same log
cycle) will have to wait for it.

The radosgw will hold a list of all the buckets that were updated in the past two cycles
and every cycle will log these entries in the updated buckets log.


Secondary
---------

* Bucket index log

The bucket index log will also hold the last primary version.

* Processing state

The sync processing state will be kept in a log. This will include latest updated buckets log id that was processed successfully.

* Full sync info

A list that contains the names of the buckets that require a full sync. It will also be spread across multiple objects.

Full Sync of System
^^^^^^^^^^^^^^^^^^^

Does the following::

 - retrieve list of all buckets, update the full sync buckets list, start processing

Processing a single 'full sync list' object::

 - if successfully locked object then:
 - (periodically, potentially in a different thread) renew lock
 - for each bucket
   - list objects (keep bucket index version retrieved on the first request to list objects).
     - for each object we get object name, version (bucket version), tag
   - read objects from primary (*), write them to local (secondary) cluster (keep object tag)
   - when done, update local bucket index with the bucket index version retrieved from primary
 - unlock object

(*) we should decide what to do in the case of tag mismatch

Continuous Update
^^^^^^^^^^^^^^^^^

A process that does the following::

 - Try to set a lock on a updated buckets log
 - if succeeded then:
    - read next log entries (but never read entries newer than current time - updated buckets log ttl)
    - for each bucket in log:
      - fetch bucket index version from local (secondary) bucket index
      - request a list of changes from remote (primary) bucket index, starting at the local bucket index version
      - if successful (remote had the requested data)
        - update local data
      - if not successful
        - add bucket to list of buckets requiring full sync
      - renew lock until done, then release lock
 - continue with the next log entry

We still need to be able to fully sync buckets that need to catch-up. So also do the following (in parallel)::

 - For each object in full sync list
 - periodically check list of buckets requiring full sync
 - if not empty:
   - for each bucket: full sync bucket (as specified above), remove bucket from list

