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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseExclusiveWriter {
    final static Logger LOG = LoggerFactory.getLogger(HBaseExclusiveWriter.class);

    private final byte[] writerSequenceNumberBytes;

    final static byte[] seqKeyBytes = Bytes.toBytes("-seqKey");
    final static byte[] seqKeyCol = Bytes.toBytes("exclusiveWriterSequence");
    final static String seqAttrName = "writerSeq";

    static byte[] sequenceNumberKey(byte[] startKey) {
        return Bytes.add(startKey, seqKeyBytes);
    }

    public static HBaseExclusiveWriter acquire(Connection conn, TableName tableName,
                                               long writerSequenceNumber) throws IOException {
        Admin admin = conn.getAdmin();
        Table table = conn.getTable(tableName);
        Set<byte[]> cfs = admin.getTableDescriptor(tableName).getFamiliesKeys();
        List<HRegionInfo> regions = admin.getTableRegions(tableName);
        List<Put> puts = new ArrayList<Put>();
        byte[] seqBytes = Bytes.toBytes(writerSequenceNumber);
        for (HRegionInfo r : regions) {
            Put p = new Put(sequenceNumberKey(r.getStartKey()));
            for (byte[] cf : cfs) {
                p.addColumn(cf, seqKeyCol, seqBytes);
            }
            puts.add(p);
        }
        table.put(puts);
        if (table instanceof HTable) {
            ((HTable)table).flushCommits();
        }

        List<HRegionInfo> regions2 = admin.getTableRegions(tableName);
        if (!regions2.containsAll(regions) ||
                !regions.containsAll(regions2)) {
            throw new IOException("Regions changed during acquisition");
        }
        return new HBaseExclusiveWriter(writerSequenceNumber);
    }

    private HBaseExclusiveWriter(long writerSequenceNumber) {
        this.writerSequenceNumberBytes = Bytes.toBytes(writerSequenceNumber);
    }

    public <T extends Mutation> T setWriterSeqNo(T mutation) {
        mutation.setAttribute(seqAttrName, writerSequenceNumberBytes);
        return mutation;
    }
}

    
