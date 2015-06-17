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
package hbaseexclusivewriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHBaseExclusiveWriter {
    final static Logger LOG = LoggerFactory.getLogger(TestHBaseExclusiveWriter.class);

    final static HBaseTestingUtility testUtil = new HBaseTestingUtility();
    final static byte[] testTable = Bytes.toBytes("testTable");
    final static TableName testTableName = TableName.valueOf(testTable);
    final static byte[] testCF = Bytes.toBytes("testCF");
    final static byte[] testColumn = Bytes.toBytes("testColumn");

    Admin admin = null;
    Random r = new Random();
    @BeforeClass
    public static void startCluster() throws Exception {
        testUtil.startMiniCluster(4);

    }

    @AfterClass
    public static void stopCluster() throws Exception {
        testUtil.shutdownMiniCluster();
    }

    @Before
    public void createTable() throws Exception {
        admin = testUtil.getHBaseAdmin();
        testUtil.getConfiguration().setInt("hbase.client.retries.number", 10);

        HTableDescriptor tableDesc = new HTableDescriptor(testTableName)
            .addCoprocessor(HBaseExclusiveWriterCoprocessor.class.getName())
            .addFamily(new HColumnDescriptor(testCF))
            .addFamily(new HColumnDescriptor(HBaseExclusiveWriter.seqKeyCF));
        admin.createTable(tableDesc);

    }

    @After
    public void dropTable() throws Exception {
        admin.disableTable(testTableName);
        admin.deleteTable(testTableName);
    }

    @Test
    public void testWriterExclusion() throws Exception {
        Connection connection1 = ConnectionFactory.createConnection(
                testUtil.getConfiguration());
        Table table1 = connection1.getTable(testTableName);

        try {
            Put p = new Put(Bytes.toBytes(r.nextInt()))
                .addColumn(testCF, testColumn, Bytes.toBytes(r.nextInt()));
            table1.put(p);
            fail("Shouldn't have been able to write without acquiring");
        } catch (IOException ioe) {
            // correct behaviour
        }

        HBaseExclusiveWriter writer1 = HBaseExclusiveWriter.acquire(
                connection1, testTableName, 1);

        Put p = writer1.setWriterSeqNo(new Put(Bytes.toBytes(r.nextInt())))
            .addColumn(testCF, testColumn, Bytes.toBytes(r.nextInt()));
        table1.put(p); // should work

        Connection connection2 = ConnectionFactory.createConnection(
                testUtil.getConfiguration());
        Table table2 = connection2.getTable(TableName.valueOf(testTable));

        HBaseExclusiveWriter writer2 = HBaseExclusiveWriter.acquire(connection2,
                                                                    testTableName, 2);

        try {
            Put p2 = writer1.setWriterSeqNo(new Put(Bytes.toBytes(r.nextInt())))
                .addColumn(testCF, testColumn, Bytes.toBytes(r.nextInt()));
            table1.put(p2);
            fail("Shouldn't be able to write when other writer has acquired");
        } catch (IOException ioe) {
            // should fail
        }

        Put p3 = writer2.setWriterSeqNo(new Put(Bytes.toBytes(r.nextInt())))
            .addColumn(testCF, testColumn, Bytes.toBytes(r.nextInt()));
        table2.put(p3); // should work
    }

    @Test
    public void testLossOfRegionServer() throws Exception {
        admin.split(testTableName, Bytes.toBytes("key100"));

        Connection connection1 = ConnectionFactory.createConnection(testUtil.getConfiguration());

        RegionLocator locator = connection1.getRegionLocator(testTableName);
        for (int i = 0; i < 10; i++) {
            if (locator.getAllRegionLocations().size() == 2) {
                break;
            }
            Thread.sleep(1000);
        }
        assertEquals(locator.getAllRegionLocations().size(), 2);

        Table table1 = connection1.getTable(TableName.valueOf(testTable));

        HBaseExclusiveWriter writer1 = HBaseExclusiveWriter.acquire(connection1,
                                                                    testTableName, 1);
        Put p = writer1.setWriterSeqNo(new Put(Bytes.toBytes(r.nextInt())))
            .addColumn(testCF, testColumn, Bytes.toBytes(r.nextInt()));
        table1.put(p); // should work

        Connection connection2 = ConnectionFactory.createConnection(testUtil.getConfiguration());
        Table table2 = connection2.getTable(TableName.valueOf(testTable));

        HBaseExclusiveWriter writer2 = HBaseExclusiveWriter.acquire(connection2,
                                                                    testTableName, 2);

        byte[] regionName = locator.getRegionLocation(Bytes.toBytes("key101"))
            .getRegionInfo().getRegionName();
        int index = testUtil.getMiniHBaseCluster().getServerWith(regionName);
        for (int i = 0; i < 10 && index == -1; i++) {
            Thread.sleep(1000);
            index = testUtil.getMiniHBaseCluster().getServerWith(regionName);
        }
        // kill region server with key200-key299
        testUtil.getMiniHBaseCluster().abortRegionServer(index);

        try {
            Put p2 = writer1.setWriterSeqNo(new Put(Bytes.toBytes("key201")))
                .addColumn(testCF, testColumn, Bytes.toBytes(r.nextInt()));
            table1.put(p2);
            fail("Shouldn't be able to write, new writer has acquired");
        } catch (IOException ioe) {
            // should fail
        }

        Put p3 = writer2.setWriterSeqNo(new Put(Bytes.toBytes("key201")))
            .addColumn(testCF, testColumn, Bytes.toBytes(r.nextInt()));
        table2.put(p3);
    }
}
