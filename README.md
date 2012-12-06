# Direct Hadoop adapter for Couchbase

Currently has:

 * InputFormat that splits by Couchbase nodes, splits are vbuckets that existed
   on each node at job start.
 * Outputs `<String,byte[]>` K/V pairs when using mapreduce. API, or
   `BytesWritable` pairs when using mapred. API
 * Cascading Tap impl, returns K/V as Strings (expects UTF-8!)
 * Reads K/Vs over Couchbase TAP protocol

TODO:

 * Output to Couchbase
 * Clean up the build stuff (don't force deps on cascading, probably other things.)
 * Tests (and run them somewhere other than my laptop!)
 * An InputFormat that reads from a view could also be nice.
