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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public class HBaseExclusiveWriterCoprocessor extends BaseRegionObserver
    implements CoprocessorService {
    final static Logger LOG = LoggerFactory.getLogger(
            HBaseExclusiveWriterCoprocessor.class);

    final static long INVALID_SEQ = -1L;
    private AtomicLong writerSequenceNumber = new AtomicLong(INVALID_SEQ);
    private volatile RegionCoprocessorEnvironment env = null;

    @Override
    public Service getService() {
        return new WriterSeqUpdate() {
            @Override
            public void update(RpcController controller,
                               UpdateWriterSeqRequest request,
                               RpcCallback<WriterSeqResponse> done) {
                WriterSeqResponse.Builder builder
                    = WriterSeqResponse.newBuilder();
                if (env == null) {
                    builder.setErrorCode(ErrorCode.NO_ENV_ERR);
                    done.run(builder.build());
                    return;
                }
                final Long newSeq;
                try {
                    newSeq = readSeq(env);
                } catch (IOException ioe) {
                    LOG.error("Error reading sequence number", ioe);
                    builder.setErrorCode(ErrorCode.CANT_READ_ERR);
                    done.run(builder.build());
                    return;
                }
                /**
                 * Write to the region. What is written doesn't matter,
                 * what matters is that this coprocessor is _able_ to
                 * write to the region, signifying that this regionserver
                 * is still responsible for the region.
                 */
                try {
                    byte[] key = env.getRegionInfo().getStartKey();
                    if (key == null || key.length == 0) {
                        // special case for first key in table
                        key = new byte[] { 0 };
                    }
                    Put p = new Put(key)
                        .addColumn(HBaseExclusiveWriter.seqKeyCF,
                                   HBaseExclusiveWriter.seqKeyCol,
                                   Bytes.toBytes(newSeq));
                    env.getRegion().put(p);
                } catch (IOException ioe) {
                    LOG.error("Error writing to region", ioe);
                    builder.setErrorCode(ErrorCode.CANT_WRITE_ERR);
                    done.run(builder.build());
                    return;
                }

                try {
                    updateSeq(newSeq);
                } catch (IOException ioe) {
                    LOG.error("Error updating sequence number", ioe);
                    builder.setErrorCode(ErrorCode.CANT_UPDATE_ERR);
                    done.run(builder.build());
                    return;
                }

                builder.setErrorCode(ErrorCode.OK);
                builder.setSeqNum(newSeq);
                done.run(builder.build());
            }
        };
    }

    public void start(CoprocessorEnvironment e)
            throws IOException {
        if (e instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)e;
        }
    }

    private void updateSeq(long newSeq) throws IOException {
        // retry if another thread is modifying the writer sequence number
        while (true) {
            long curSeq = writerSequenceNumber.get();
            if (newSeq < curSeq) {
                throw new IOException(
                        "New sequence number(" + newSeq + ") lower than "
                        + "current sequence number(" + curSeq+ ")");
            } else if (!writerSequenceNumber.compareAndSet(curSeq, newSeq)) {
                continue;
            } else {
                break;
            }
        }
    }

    private long readSeq(RegionCoprocessorEnvironment env) throws IOException {
        Table table = env.getTable(env.getRegionInfo().getTable());
        Get get = new Get(HBaseExclusiveWriter.seqKeyRow)
            .addColumn(HBaseExclusiveWriter.seqKeyCF,
                       HBaseExclusiveWriter.seqKeyCol);
        byte[] newSeqBytes = table.get(get).getValue(
                HBaseExclusiveWriter.seqKeyCF,
                HBaseExclusiveWriter.seqKeyCol);
        if (newSeqBytes != null) {
            long newSeq = Bytes.toLong(newSeqBytes);
            return newSeq;
        } else {
            throw new IOException("Couldn't read sequence");
        }
    }

    private void checkWriteSequenceNumber(RegionCoprocessorEnvironment env, Mutation mutation)
            throws IOException {
        for (byte[] cf : mutation.getFamilyCellMap().keySet()) {
            if (Bytes.equals(cf, HBaseExclusiveWriter.seqKeyCF)) {
                return;
            }
        }
        if (writerSequenceNumber.get() == INVALID_SEQ) {
            updateSeq(readSeq(env));
        }
        byte[] attr = mutation.getAttribute(HBaseExclusiveWriter.seqAttrName);
        if (attr == null) {
            throw new IOException("Writer sequence not set");
        }
        long writeSeq = Bytes.toLong(attr);

        if (writeSeq < writerSequenceNumber.get()) {
            throw new IOException("Writer sequence too old.");
        }
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
                       final Put put, final WALEdit edit,
                       final Durability durability) throws IOException {
        checkWriteSequenceNumber(env, put);
    }

    @Override
    public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> e, final Delete delete,
                          final WALEdit edit, final Durability durability) throws IOException {
        checkWriteSequenceNumber(e.getEnvironment(), delete);
    }

    @Override
    public void preBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> e,
                               final MiniBatchOperationInProgress<Mutation> miniBatchOp)
            throws IOException {
        for (int i = 0; i < miniBatchOp.size(); i++) {
            Mutation m = miniBatchOp.getOperation(i);
            checkWriteSequenceNumber(e.getEnvironment(), m);
        }
    }

    @Override
    public boolean preCheckAndPut(final ObserverContext<RegionCoprocessorEnvironment> e,
                                  final byte [] row, final byte [] family, final byte [] qualifier,
                                  final CompareOp compareOp, final ByteArrayComparable comparator,
                                  final Put put, final boolean result) throws IOException {
        checkWriteSequenceNumber(e.getEnvironment(), put);
        return result;
    }

    @Override
    public boolean preCheckAndPutAfterRowLock(final ObserverContext<RegionCoprocessorEnvironment> e,
                                              final byte[] row, final byte[] family,
                                              final byte[] qualifier, final CompareOp compareOp,
                                              final ByteArrayComparable comparator, final Put put,
                                              final boolean result) throws IOException {
        checkWriteSequenceNumber(e.getEnvironment(), put);
        return result;
    }

    @Override
    public boolean preCheckAndDelete(final ObserverContext<RegionCoprocessorEnvironment> e,
                                     final byte [] row, final byte [] family, final byte [] qualifier,
                                     final CompareOp compareOp, final ByteArrayComparable comparator,
                                     final Delete delete, final boolean result) throws IOException {
        checkWriteSequenceNumber(e.getEnvironment(), delete);
        return result;
    }

    @Override
    public boolean preCheckAndDeleteAfterRowLock(final ObserverContext<RegionCoprocessorEnvironment> e,
                                                 final byte[] row, final byte[] family,
                                                 final byte[] qualifier, final CompareOp compareOp,
                                                 final ByteArrayComparable comparator,
                                                 final Delete delete,
                                                 final boolean result) throws IOException {
        checkWriteSequenceNumber(e.getEnvironment(), delete);
        return result;
    }

    @Override
    public Result preAppend(final ObserverContext<RegionCoprocessorEnvironment> e,
                            final Append append) throws IOException {
        checkWriteSequenceNumber(e.getEnvironment(), append);
        return null;
    }

    @Override
    public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
                               final Increment increment) throws IOException {
        checkWriteSequenceNumber(e.getEnvironment(), increment);
        return null;
    }
}

