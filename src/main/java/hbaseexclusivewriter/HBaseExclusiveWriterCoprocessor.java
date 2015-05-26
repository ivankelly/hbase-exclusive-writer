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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseExclusiveWriterCoprocessor extends BaseRegionObserver {
    final static Logger LOG = LoggerFactory.getLogger(HBaseExclusiveWriterCoprocessor.class);
    
    final static long INVALID_SEQ = -1L;
    private AtomicLong writerSequenceNumber = new AtomicLong(INVALID_SEQ);

    private long readSeqFromRegion(RegionCoprocessorEnvironment env) throws IOException {
        byte[] key = HBaseExclusiveWriter.sequenceNumberKey(
                env.getRegionInfo().getStartKey());
        Set<byte[]> cfs = env.getTable(env.getRegionInfo().getTable())
            .getTableDescriptor().getFamiliesKeys();
        long seq = INVALID_SEQ;
        Get g = new Get(key);
        for (byte[] cf : cfs) {
            g.addColumn(cf, HBaseExclusiveWriter.seqKeyCol).setMaxVersions(1);
        }
        Result r = env.getRegion().get(g);
        for (byte[] cf : cfs) {
            if (!r.containsColumn(cf, HBaseExclusiveWriter.seqKeyCol)) {
                throw new IOException("Region has never been acquired");
            }
            byte[] cellValue = CellUtil.cloneValue(r.getColumnLatestCell(cf,
                                                            HBaseExclusiveWriter.seqKeyCol));
            long newSeq = Bytes.toLong(cellValue);
            if (newSeq > seq) {
                seq = newSeq;
            }
        }
        return seq;
    }
    
    private void checkWriteSequenceNumber(RegionCoprocessorEnvironment env, Mutation mutation)
            throws IOException {
        if (writerSequenceNumber.get() == INVALID_SEQ) {
            long seq = readSeqFromRegion(env);
            if (!writerSequenceNumber.compareAndSet(INVALID_SEQ, seq)) {
                LOG.warn("Couldn't set writer seq, other read must have done so");
            }
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
        RegionCoprocessorEnvironment env = e.getEnvironment();
        byte[] seqKey = HBaseExclusiveWriter.sequenceNumberKey(
                env.getRegionInfo().getStartKey());
        long currSeq = writerSequenceNumber.get();
        if (Bytes.equals(put.getRow(), seqKey)) {
            Set<byte[]> cfs = env.getTable(env.getRegionInfo().getTable())
                .getTableDescriptor().getFamiliesKeys();
            long newSeq = currSeq;
            for (byte[] cf : cfs) {
                List<Cell> cells = put.get(cf, HBaseExclusiveWriter.seqKeyCol);
                for (Cell c : cells) {
                    long seq = Bytes.toLong(c.getValue());
                    if (seq > newSeq) {
                        newSeq = seq;
                    }
                }
            }
            if (newSeq > currSeq) {
                if (!writerSequenceNumber.compareAndSet(currSeq, newSeq)) {
                    throw new IOException("Concurrent writer, check ownership");
                }
            }
        } else {
            checkWriteSequenceNumber(env, put);
        }
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
        RegionCoprocessorEnvironment env = e.getEnvironment();
        byte[] seqKey = HBaseExclusiveWriter.sequenceNumberKey(
                env.getRegionInfo().getStartKey());
        
        for (int i = 0; i < miniBatchOp.size(); i++) {
            Mutation m = miniBatchOp.getOperation(i);
            if (!Bytes.equals(m.getRow(), seqKey)) {
                checkWriteSequenceNumber(e.getEnvironment(), m);
            }
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

