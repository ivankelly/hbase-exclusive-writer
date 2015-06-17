An example client, to demonstrate the usage of hbase exclusive
writer. Multiple instances of the client should be run simulaneously,
with the leader being paused every so often to allow another leader to
take over. The paused process should then be woken up.

It requires a running hbase cluster to work. A standalone or
pseudo-distributed cluster is fine.

# Building

To build:
```
$ ../gradlew jar
```

This will create example-client.jar which is a runnable fatjar in
build/libs.

# Demonstrating split brain

Set up the a table in hbase without the exclusive writer coprocessor
enabled.

```
hbase(main):010:0> create 'NonExclusiveTable', { NAME => 'TEST', VERSIONS => 1 }
```

Then open multiple terminal windows and run the command:
```
$ java -jar build/libs/example-client.jar -run -table NonExclusiveTable -skipExclusiveWriter
```

One of the instances will become leader and start writing to the
table.

```
Leader selector started
I am leader, writing to hbase
Writing with sequence number 116
Starting writing from row 0
.........10 entries written
.........20 entries written
.........30 entries written
...
```

Let this run for a few seconds, and then pause the process with
^Z. One of the other instances will then become leader and start
writing to the table. Wake up the paused process using the fg command
at the terminal.

```
.........90 entries written
..^Z
[1]+  Stopped                 java -jar build/libs/example-client.jar -run -table NonExclusiveTable -skipExclusiveWriter
$ fg
java -jar build/libs/example-client.jar -run -table NonExclusiveTable -skipExclusiveWriter
.2015-06-15 17:53:28,655 ERROR [Write-Thread] client.AsyncProcess$AsyncRequestFutureImpl(927): Cannot get replica 0 location for {"totalColumns":1,"families":{"TEST":[{"timestamp":9223372036854775807,"tag":[],"qualifier":"SEQ\\x00\\x00\\x00\\x00\\x00\\x00\\x00o","vlen":8}]},"row":"\\x00\\x00\\x00\\x00\\x00\\x00\\x02v"}
Write thread threw exception
org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException: Failed 1 action: IOException: 1 time, 
	at org.apache.hadoop.hbase.client.AsyncProcess$BatchErrors.makeException(AsyncProcess.java:228)
...
```

Do this a couple of times to the leader. We are trying to trigger a
race condition, so there's no guarantee that split brain will occur
_every_ time.

Once you have paused and unpaused the leader a couple of times, run
the verifier on the table. If split brain has occurred, you should get
an output something like:

```
$ java -jar example-client.jar -verify -table NonExclusiveTable 
Verifying the database
Exception in thread "main" java.lang.Exception: Row 59 written to by more than one writer 107, 108
	at ExampleClient.verify(ExampleClient.java:219)
	at ExampleClient.main(ExampleClient.java:243)
```
	
# Demonstrating the exclusive writer

To demonstrate the exclusive writer, the table needs to have an
'exclusiveWriter' column family, and the HBase Exclusive Writer
coprocessor needs to be enabled.

```
hbase(main):006:0> create 'ExclusiveTable', { NAME => 'TEST', VERSIONS => 1 }, { NAME => 'exclusiveWriter', VERSIONS => 1 }
0 row(s) in 1.2170 seconds

=> Hbase::Table - ExclusiveTable
hbase(main):006:0> alter 'ExclusiveTable', METHOD => 'table_att', 'coprocessor' => 'hdfs:///coprocessors/hbase-exclusive-writer.jar|hbaseexclusivewriter.HBaseExclusiveWriterCoprocessor|1000'
Updating all regions with the new schema...
0/1 regions updated.
1/1 regions updated.
Done.
0 row(s) in 2.9970 seconds
```

Now run multiple instances of the client with the command:

```
$ java -jar build/libs/example-client.jar -run -table ExclusiveTable
```

Now pause and unpause the leader multiple times. Once you have done
this for long enough, verify the table. There will be no rows which
have been written to more than once.

```
$ java -jar build/libs/example-client.jar -verify -table ExclusiveTable
Verifying the database
It all checks out!
```
