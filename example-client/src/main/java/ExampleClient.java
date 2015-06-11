import java.io.Closeable;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.leader.LeaderSelector;

import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.ExponentialBackoffRetry;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.JCommander;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import hbaseexclusivewriter.HBaseExclusiveWriter;

public class ExampleClient extends LeaderSelectorListenerAdapter implements Closeable {
    static final byte[] CF = Bytes.toBytes("TEST");
    static final byte[] COLUMN = Bytes.toBytes("TEST");

    final String zk;
    final TableName tableName;

    final CuratorFramework client;
    final Configuration hbaseConf;
    
    ExampleClient(String zk, String table) {
        this.zk = zk;
        this.tableName = TableName.valueOf(table);

        client = CuratorFrameworkFactory.builder()
            .connectString(zk)
            .connectionTimeoutMs(1000)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .sessionTimeoutMs(5000).build();
        client.start();

        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", zk);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
    
    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        // we are now the leader. This method should not return until we want to relinquish leadership
        System.out.println("I am leader, writing to hbase");
        DistributedAtomicLong seqGenerator = new DistributedAtomicLong(client,
                "/exampleSequencer", new ExponentialBackoffRetry(1000, 3));
        seqGenerator.initialize(0L); // only does anything if sequencer was never initialized
        AtomicValue<Long> oldValue = seqGenerator.get();
        long mySequence = oldValue.postValue() + 1;
        System.out.println("oldValue " + oldValue.postValue() + " new value " + mySequence + " succeeded " + oldValue.succeeded());
        AtomicValue<Long> atomic = seqGenerator.compareAndSet(oldValue.postValue(),
                                                              mySequence);
        if (atomic.succeeded()) {
            System.err.println("Writing with sequence number " + mySequence);
            Connection conn = ConnectionFactory.createConnection(hbaseConf);
            Table table = conn.getTable(tableName);

            HBaseExclusiveWriter writer = HBaseExclusiveWriter.acquire(
                    conn, tableName, mySequence);
            while (true) {
                long time = System.currentTimeMillis();
                Put p = new Put(Bytes.toBytes(time));
                p.addColumn(CF, COLUMN, Bytes.toBytes(mySequence));
                Thread.sleep(100);
            }
        } else {
            System.err.println("Failed to update sequence number");
        }
    }
   
    public void run() throws Exception {
        LeaderSelector leaderSelector = new LeaderSelector(client, "/exampleLeader", this);
          
        // for most cases you will want your instance to requeue when it relinquishes leadership
        leaderSelector.autoRequeue();
        leaderSelector.start();
        while (true) {
            Thread.sleep(1000);
        }
    }

    public void verify() throws Exception {
        Connection conn = ConnectionFactory.createConnection(hbaseConf);
        Table table = conn.getTable(tableName);

        ResultScanner scanner = table.getScanner(CF, COLUMN);
        Result r = scanner.next();
        long curSeq = -1;
        while (r != null) {
            long rowSeq = Bytes.toLong(r.getValue(CF, COLUMN));
            if (rowSeq < curSeq) {
                throw new Exception("Found a write from "
                        + rowSeq + " later than a write from " + curSeq);
            }
            r = scanner.next();
        }
        System.out.println("It all checks out!");
    }
    
    public static void main(String[] args) throws Exception {
        CmdLine cmd = new CmdLine();
        new JCommander(cmd, args);

        ExampleClient client = new ExampleClient(cmd.zk, cmd.table);
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

        @Parameter(names = "-run", description = "Run an example client")
        boolean run = false;

        @Parameter(names = "-verify", description = "Verify writers have been exclusive")
        boolean verify = false;
    }
}
