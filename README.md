# Direct Hadoop adapter for Couchbase

Currently has:

 * InputFormat that splits by Couchbase nodes, splits are vbuckets that existed
   on each node at job start.
 * Outputs `<String,byte[]>` K/V pairs
 * Reads K/Vs over TAP

TODO:

 * OutputFormat!
 * Tests (and run them somewhere other than my laptop!)
 * An InputFormat that reads from a view could also be nice.
