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

import hbaseexclusivewriter.proto.WriterSeqProto.ErrorCode;
import hbaseexclusivewriter.proto.WriterSeqProto.UpdateWriterSeqRequest;
import hbaseexclusivewriter.proto.WriterSeqProto.WriterSeqResponse;
import hbaseexclusivewriter.proto.WriterSeqProto.WriterSeqUpdate;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseExclusiveWriter {
    final static Logger LOG = LoggerFactory.getLogger(HBaseExclusiveWriter.class);

    private final byte[] writerSequenceNumberBytes;

    public final static byte[] seqKeyRow = Bytes.toBytes("seqKey");
    public final static byte[] seqKeyCF = Bytes.toBytes("exclusiveWriter");
    public final static byte[] seqKeyCol = Bytes.toBytes("exclusiveWriterSequence");
    final static String seqAttrName = "writerSeq";

    static byte[] sequenceNumberKey(byte[] startKey) {
        return Bytes.add(startKey, seqKeyRow);
    }

    public static HBaseExclusiveWriter acquire(Connection conn,
                                               TableName tableName,
                                               long writerSequenceNumber)
            throws IOException {
        Admin admin = conn.getAdmin();
        Table table = conn.getTable(tableName);

        // get current value
        Get get = new Get(seqKeyRow).addColumn(seqKeyCF, seqKeyCol);
        byte[] currentSeqBytes = table.get(get).getValue(seqKeyCF, seqKeyCol);
        if (currentSeqBytes != null) {
            Long currentSeq  = Bytes.toLong(currentSeqBytes);
            if (currentSeq > writerSequenceNumber) {
                throw new IOException(
                        "Current table writer sequence is higher that requested sequence");
            }
        }
        Put put = new Put(seqKeyRow).addColumn(seqKeyCF, seqKeyCol,
                                               Bytes.toBytes(writerSequenceNumber));
        table.put(put);
        if (table instanceof HTable) {
            ((HTable)table).flushCommits();
        }

        List<HRegionInfo> regions = admin.getTableRegions(tableName);
        final UpdateWriterSeqRequest req = UpdateWriterSeqRequest
            .newBuilder().build();
        final ServerRpcController controller = new ServerRpcController();
        try {
            final AtomicInteger errors = new AtomicInteger(0);
            Map<byte[], WriterSeqResponse> results
                = table.coprocessorService(WriterSeqUpdate.class,
                        null, null,
                        new Batch.Call<WriterSeqUpdate, WriterSeqResponse>() {
                    @Override
                    public WriterSeqResponse call(
                            WriterSeqUpdate service) throws IOException {
                        try {
                            BlockingRpcCallback<WriterSeqResponse> rpcCallback = new BlockingRpcCallback<WriterSeqResponse>();
                            service.update(controller, req, rpcCallback);
                            WriterSeqResponse ret = rpcCallback.get();
                            if (ret == null) {
                                errors.incrementAndGet();
                            }
                            return ret;
                        } catch (Throwable t) {
                            errors.incrementAndGet();
                            throw t;
                        }
                    }
                });
            if (errors.get() > 0) {
                throw new IOException(
                        "Error contacting exclusive writer service");
            }
            for (Map.Entry<byte[],WriterSeqResponse> r : results.entrySet()) {
                if (r.getValue().getErrorCode() == ErrorCode.OK
                    && r.getValue().hasSeqNum()) {
                    if (r.getValue().getSeqNum() != writerSequenceNumber) {
                        throw new IOException("Sequence number not updated "
                                + "(wanted: " + writerSequenceNumber + ", got: "
                                + r.getValue().getSeqNum() + ")");
                    }
                } else {
                    throw new IOException("Error " + r.getValue().getErrorCode()
                                          + " updating writer sequence on region "
                                          + Bytes.toString(r.getKey()));
                }
            }
        } catch (Throwable t) {
            throw new IOException("Exception accessing writer seq service", t);
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
