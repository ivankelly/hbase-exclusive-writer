/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import hbaseexclusivewriter.HBaseExclusiveWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Joiner;

/**
 * Example client to demonstrate hbase exclusive writer.
 * Requires a table with 2 column families, TEST and exclusiveWriter.
 * HBaseExclusiveWriterCoprocessor also needs to be enabled for the
 * table.
 *
 * The client uses the curator leader selector recipe for leader election,
 * and DistributedAtomicLong to generate writer sequence numbers. Once
 * an instance of the client takes leadership, it will start writing
 * sequentially to the hbase table every 100 milliseconds.
 *
 * The client has 2 modes, run and verify. Run will try to become leader
 * and write to the table, verify will check that only one leader has been
 * able to write to the table at any one time.
 */
public class ExampleClient
    extends LeaderSelectorListenerAdapter implements Closeable {
    static final byte[] CF = Bytes.toBytes("TEST");
    static final byte[] SEQ_COLUMN_PREFIX = Bytes.toBytes("SEQ");

    final String zk;
    final TableName tableName;

    final CuratorFramework client;
    final Configuration hbaseConf;
    final boolean useExclusiveWriter;

    ExampleClient(String zk, String table, boolean useExclusiveWriter) {
        this.zk = zk;
        this.tableName = TableName.valueOf(table);
        this.useExclusiveWriter = useExclusiveWriter;

        client = CuratorFrameworkFactory.builder()
            .connectString(zk)
            .connectionTimeoutMs(1000)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .sessionTimeoutMs(5000).build();
        client.start();

        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", zk);
        hbaseConf.setInt("hbase.client.retries.number", 5);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private long getLastRow(Table table) throws IOException {
        ResultScanner scanner = table.getScanner(
                new Scan().addFamily(CF).setReversed(true));
        try {
            Result r = scanner.next();
            if (r == null) {
                return -1;
            } else {
                return Bytes.toLong(r.getRow());
            }
        } finally {
            scanner.close();
        }
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        // we are now the leader. This method should not return until we want
        // to relinquish leadership
        System.out.println("I am leader, writing to hbase");

        // generate a writer sequence number
        DistributedAtomicLong seqGenerator = new DistributedAtomicLong(client,
                "/exampleSequencer", new ExponentialBackoffRetry(1000, 3));
        // initialize only does something if sequencer was never initialized
        seqGenerator.initialize(0L);

        AtomicValue<Long> oldValue = seqGenerator.get();
        final long mySequence = oldValue.postValue() + 1;
        AtomicValue<Long> atomic = seqGenerator.compareAndSet(
                oldValue.postValue(), mySequence);
        if (atomic.succeeded()) {
            System.err.println("Writing with sequence number " + mySequence);

            // starting to write to hbase
            Connection conn = ConnectionFactory.createConnection(hbaseConf);
            final Table table = conn.getTable(tableName);

            // acquire the exclusive writer lock
            final HBaseExclusiveWriter writer;
            if (useExclusiveWriter) {
                writer = HBaseExclusiveWriter.acquire(
                        conn, tableName, mySequence);
            } else {
                writer = null;
            }

            long lastRow = getLastRow(table);
            final long firstRow = lastRow + 1;

            final AtomicBoolean running = new AtomicBoolean(true);

            /**
             * Create the writer as a new thread rather than
             * doing it directly in this thread. This allows us
             * to catch the interrupt from the leader selector when
             * we loose leadership and manually stop writing.
             * This is done like this to increase the chance of a
             * race occurring where we continue writing after loosing
             * leadership. This _can_ happen even without this delay,
             * but it's harder to reliably demonstrate.
             */
            Thread t = new Thread("Write-Thread") {
                    @Override
                    public void run() {
                        int i = 0;
                        long newRow = firstRow;
                        System.out.println("Starting writing from row "
                                           + firstRow);
                        while (running.get()) {
                            try {
                                Thread.sleep(100);

                                Put p = new Put(Bytes.toBytes(newRow));
                                if (writer != null) {
                                    writer.setWriterSeqNo(p);
                                }
                                byte[] seqBytes = Bytes.toBytes(mySequence);
                                p.addColumn(CF,
                                            Bytes.add(SEQ_COLUMN_PREFIX,
                                                      seqBytes),
                                            seqBytes);
                                table.put(p);

                                newRow++;

                                if (++i % 10 == 0 && i > 0) {
                                    System.out.println(i + " entries written");
                                } else {
                                    System.out.print(".");
                                }
                            } catch (Throwable t) {
                                System.err.println(
                                        "Write thread threw exception");
                                t.printStackTrace(System.err);
                            }
                        }
                    }
                };

            try {
                t.start();
                t.join();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                running.set(false);
                t.join();
            } finally {
                table.close();
                conn.close();
            }
        } else {
            System.err.println("Failed to update sequence number");
        }
    }

    public void run() throws Exception {
        LeaderSelector leaderSelector
            = new LeaderSelector(client, "/exampleLeader", this);
        try {
            leaderSelector.autoRequeue();
            leaderSelector.start();
            System.out.println("Leader selector started");
            while (true) {
                Thread.sleep(1000);
            }
        } finally {
            leaderSelector.close();
        }
    }

    public void verify() throws Exception {
        Connection conn = ConnectionFactory.createConnection(hbaseConf);
        Table table = conn.getTable(tableName);

        ResultScanner scanner = table.getScanner(CF);
        Result r = scanner.next();
        while (r != null) {
            List<Cell> cells = r.listCells();
            List<Long> seqQualifiers = new ArrayList<Long>();
            for (Cell c : cells) {
                byte[] qualifier = CellUtil.cloneQualifier(c);
                if (Bytes.startsWith(qualifier, SEQ_COLUMN_PREFIX)) {
                    seqQualifiers.add(Bytes.toLong(qualifier,
                                              SEQ_COLUMN_PREFIX.length, 8));
                }
            }
            if (seqQualifiers.size() > 1) {
                throw new Exception("Row " + Bytes.toLong(r.getRow())
                                    + " written to by more than one writer "
                                    + Joiner.on(", ").join(seqQualifiers));
            }
            r = scanner.next();
        }
        System.out.println("It all checks out!");
    }

    public static void main(String[] args) throws Exception {
        CmdLine cmd = new CmdLine();
        JCommander cmdr = new JCommander(cmd, args);

        if (cmd.help || (!cmd.run && !cmd.verify)) {
            cmdr.usage();
            System.exit(1);
        }
        ExampleClient client = new ExampleClient(cmd.zk, cmd.table,
                                                 !cmd.skipExclusiveWriter);
        if (cmd.run) {
            System.out.println("Running a client");
            client.run();
        } else if (cmd.verify) {
            System.out.println("Verifying the database");
            client.verify();
        }
        client.close();
    }

    static class CmdLine {
        @Parameter(names = "-zk", description = "ZooKeeper server list")
        String zk = "localhost:2181";

        @Parameter(names = "-table", description = "Table to write to")
        String table = "ExclusiveTable";

        @Parameter(names = "-skipExclusiveWriter",
                   description = "Skip writer exclusion")
        boolean skipExclusiveWriter = false;

        @Parameter(names = "-run", description = "Run an example client")
        boolean run = false;

        @Parameter(names = "-verify",
                   description = "Verify writers have been exclusive")
        boolean verify = false;

        @Parameter(names = "-help",
                   description = "This help message", help = true)
        boolean help;
    }
}
