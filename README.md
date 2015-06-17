# HBase Exclusive Writer

Allows processes writing to HBase table to forceably exclude all other writers.

## Why is this useful?

Commonly people try to do this using distributed locks in something
like ZooKeeper. However, this alone is not sufficient to guarantee
that only one process will write to the table at a time. A process
could acquire the lock from zookeeper and immediately go into a GC
pause/get partitioned from the network. If another process takes over
the lock, when the original process wakes up, it will be will still
think it has the lock and will be able to write to the table. It will
eventually be notified that it no longer has the lock, but by then it
could already have written to the table outside of the lock.

The solution to this is to enforce the lock with a writer sequence
number. To allow a write, the write must contain a sequence number
greater or equal to any previously seen sequence number. To block a
previous writer, the new writer must make sure that it sends a new
sequence number to enough nodes that the previous writer will not be
able to complete a write without contacting one of those nodes.

In the case of hbase, this means contacting _every_ regionserver which
holds a region for the table. While this could be considered a problem
for availability, HBase already endevours to keep all regions
available at all times. If a region is unavailable you could consider
the table itself to be unavailable.

# Building

```
$ ./gradlew build
```

This will produce a jar, build/libs/hbase-exclusive-writer.jar. This
jar contains the client and the coprocessor.

# How do I use it?

There are two parts, a coprocessor and a client.

The client is used to acquire a lock on the table using a writer
sequence number and then attach the sequence number to all writes to
the table.

The coprocessor runs on each region and enforces that only the writes
with the same sequence number as the latest sequence number it has
seen are accepted.

# Enabling exclusive writes on a table

The table needs to have a 'exclusiveWriter' column family, and the
coprocessor needs to be enabled.

```
hbase(main):008:0> alter 'ExclusiveTable', { NAME => 'exclusiveWriter', VERSIONS => 1 }
hbase(main):009:0> alter 'ExclusiveTable', METHOD => 'table_att', 'coprocessor' => 'hdfs:///coprocessors/hbase-exclusive-writer.jar|hbaseexclusivewriter.HBaseExclusiveWriterCoprocessor|1000'
```

# Acquiring and writing to a table

To acquire a table, you first need to generate a sequence number
higher than any sequence number that has written to the table. A
simple way to do this is to use [Curator's](http://curator.apache.org)
DistributedAtomicLong recipe.

Once you have a writer sequence number, you can acquire the table.

```
Connection conn = ConnectionFactory.createConnection(hbaseConf);
TableName tableName = TableName.valueOf("FOOBAR");
HBaseExclusiveWriter writer = HBaseExclusiveWriter.acquire(conn, tableName, mySequence);
```

All writes to the table must then be tags with the writer sequence
number.

```
Put p = writer.setWriterSeqNo(new Put(Bytes.toBytes("myRow")));
p.addColumn(FAMILY, QUALIFIER, value);
table.put(p);
```

# Example usage

There is a example client in the example-client/ subdirectory. The
README.md there explains its usage.

This project is distributed under the Apache Software Foundation License, Version 2.
