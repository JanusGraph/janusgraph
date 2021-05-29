// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.database.log;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.log.Change;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.HashingUtil;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.log.StandardTransactionId;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.TransactionConfiguration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TransactionLogHeader {

    private final long transactionId;
    private final Instant txTimestamp;
    private final TimestampProvider times;
    private final StaticBuffer logKey;

    public TransactionLogHeader(long transactionId, Instant txTimestamp, TimestampProvider times) {
        this.transactionId = transactionId;
        this.txTimestamp = txTimestamp;
        this.times = times;
        Preconditions.checkArgument(this.transactionId > 0);
        Preconditions.checkNotNull(this.txTimestamp);
        logKey = HashingUtil.hashPrefixKey(HashingUtil.HashLength.SHORT, BufferUtil.getLongBuffer(transactionId));
    }


    public long getId() {
        return transactionId;
    }

    public Instant getTimestamp() {
        return txTimestamp;
    }

    public StaticBuffer getLogKey() {
        return logKey;
    }

    public StaticBuffer serializeModifications(Serializer serializer, LogTxStatus status, StandardJanusGraphTx tx,
                                               final Collection<InternalRelation> addedRelations,
                                               final Collection<InternalRelation> deletedRelations) {
        Preconditions.checkArgument(status==LogTxStatus.PRECOMMIT || status==LogTxStatus.USER_LOG);
        DataOutput out = serializeHeader(serializer, 256 + (addedRelations.size() + deletedRelations.size()) * 40, status, status == LogTxStatus.PRECOMMIT ? tx.getConfiguration() : null);
        logRelations(out, addedRelations, tx);
        logRelations(out, deletedRelations,tx);
        return out.getStaticBuffer();
    }

    private static void logRelations(DataOutput out, final Collection<InternalRelation> relations, StandardJanusGraphTx tx) {
        VariableLong.writePositive(out,relations.size());
        for (InternalRelation rel : relations) {
            VariableLong.writePositive(out,rel.getVertex(0).longId());
            org.janusgraph.diskstorage.Entry entry = tx.getEdgeSerializer().writeRelation(rel, 0, tx);
            BufferUtil.writeEntry(out,entry);
        }
    }

    public StaticBuffer serializeUserLog(Serializer serializer, Entry sourceTxEntry, StandardTransactionId sourceTxId) {
        Preconditions.checkArgument(sourceTxEntry!=null && sourceTxEntry.status==LogTxStatus.PRECOMMIT
                && sourceTxEntry.header.transactionId==sourceTxId.getTransactionId());
        StaticBuffer sourceContent = sourceTxEntry.content;
        Preconditions.checkArgument(sourceContent!=null && sourceContent.length()>0);
        final EnumMap<LogTxMeta, Object> meta = new EnumMap<>(LogTxMeta.class);
        meta.put(LogTxMeta.SOURCE_TRANSACTION,sourceTxId);
        DataOutput out = serializeHeader(serializer, 50 + sourceContent.length(), LogTxStatus.USER_LOG, meta);
        out.putBytes(sourceContent);
        return out.getStaticBuffer();
    }

    public StaticBuffer serializePrimary(Serializer serializer, LogTxStatus status) {
        Preconditions.checkArgument(status==LogTxStatus.PRIMARY_SUCCESS || status==LogTxStatus.COMPLETE_SUCCESS);
        final DataOutput out = serializeHeader(serializer, status);
        return out.getStaticBuffer();
    }

    public StaticBuffer serializeSecondary(Serializer serializer, LogTxStatus status,
                                           Map<String,Throwable> indexFailures, boolean userLogSuccess) {
        Preconditions.checkArgument(status==LogTxStatus.SECONDARY_SUCCESS || status==LogTxStatus.SECONDARY_FAILURE);
        final DataOutput out = serializeHeader(serializer, status);
        if (status==LogTxStatus.SECONDARY_FAILURE) {
            out.putBoolean(userLogSuccess);
            out.putInt(indexFailures.size());
            for (String index : indexFailures.keySet()) {
                assert StringUtils.isNotBlank(index);
                out.writeObjectNotNull(index);
            }
        } else assert userLogSuccess && indexFailures.isEmpty();
        return out.getStaticBuffer();
    }

    private DataOutput serializeHeader(Serializer serializer, LogTxStatus status) {
        return serializeHeader(serializer, 30, status, new EnumMap<>(LogTxMeta.class));
    }

    private DataOutput serializeHeader(Serializer serializer, int capacity, LogTxStatus status, TransactionConfiguration txConfig) {
        final EnumMap<LogTxMeta,Object> metaMap = new EnumMap<>(LogTxMeta.class);
        if (txConfig!=null) {
            for (LogTxMeta meta : LogTxMeta.values()) {
                Object value = meta.getValue(txConfig);
                if (value!=null) {
                    metaMap.put(meta,value);
                }
            }
        }
        return serializeHeader(serializer,capacity,status,metaMap);
    }


    private DataOutput serializeHeader(Serializer serializer, int capacity, LogTxStatus status, EnumMap<LogTxMeta,Object> meta) {
        Preconditions.checkArgument(status!=null && meta!=null,"Invalid status or meta");
        DataOutput out = serializer.getDataOutput(capacity);
        out.putLong(times.getTime(txTimestamp));
        VariableLong.writePositive(out, transactionId);
        out.writeObjectNotNull(status);

        Preconditions.checkArgument(meta.size()<Byte.MAX_VALUE,"Too much meta data: %s",meta.size());
        out.putByte(VariableLong.unsignedByte(meta.size()));
        for (Map.Entry<LogTxMeta,Object> metaEntry : meta.entrySet()) {
            assert metaEntry.getValue()!=null;
            out.putByte(VariableLong.unsignedByte(metaEntry.getKey().ordinal()));
            out.writeObjectNotNull(metaEntry.getValue());
        }
        return out;
    }

    public static Entry parse(StaticBuffer buffer, Serializer serializer, TimestampProvider times) {
        ReadBuffer read = buffer.asReadBuffer();
        Instant txTimestamp = times.getTime(read.getLong());
        TransactionLogHeader header = new TransactionLogHeader(VariableLong.readPositive(read),
                txTimestamp, times);
        LogTxStatus status = serializer.readObjectNotNull(read,LogTxStatus.class);
        final EnumMap<LogTxMeta,Object> metadata = new EnumMap<>(LogTxMeta.class);
        int metaSize = VariableLong.unsignedByte(read.getByte());
        for (int i=0;i<metaSize;i++) {
            LogTxMeta meta = LogTxMeta.values()[VariableLong.unsignedByte(read.getByte())];
            metadata.put(meta,serializer.readObjectNotNull(read,meta.dataType()));
        }
        if (read.hasRemaining()) {
            StaticBuffer content = read.subrange(read.getPosition(),read.length()-read.getPosition());
            return new Entry(header,content, status,metadata);
        } else {
            return new Entry(header,null, status,metadata);
        }
    }

    public static class Entry {

        private final TransactionLogHeader header;
        private final StaticBuffer content;
        private final LogTxStatus status;
        private final EnumMap<LogTxMeta,Object> metadata;

        public Entry(TransactionLogHeader header, StaticBuffer content, LogTxStatus status,
                     EnumMap<LogTxMeta,Object> metadata) {
            Preconditions.checkArgument(status != null && metadata != null);
            Preconditions.checkArgument(header!=null);
            Preconditions.checkArgument(content==null || content.length()>0);
            this.header = header;
            this.content = content;
            this.status = status;
            this.metadata=metadata;
        }

        public TransactionLogHeader getHeader() {
            return header;
        }

        public boolean hasContent() {
            return content!=null;
        }

        public LogTxStatus getStatus() {
            return status;
        }

        public EnumMap<LogTxMeta,Object> getMetadata() {
            return metadata;
        }

        public StaticBuffer getContent() {
            Preconditions.checkState(hasContent(),"Does not have any content");
            return content;
        }

        public SecondaryFailures getContentAsSecondaryFailures(Serializer serializer) {
            Preconditions.checkArgument(status==LogTxStatus.SECONDARY_FAILURE);
            return new SecondaryFailures(content,serializer);
        }

        public Collection<Modification> getContentAsModifications(Serializer serializer) {
            Preconditions.checkArgument(status==LogTxStatus.PRECOMMIT || status==LogTxStatus.USER_LOG);
            ReadBuffer in = content.asReadBuffer();
            List<Modification> addedModifications = readModifications(Change.ADDED,in,serializer);
            List<Modification> removedModifications = readModifications(Change.REMOVED,in,serializer);
            List<Modification> mods = new ArrayList<>(addedModifications.size()+removedModifications.size());
            mods.addAll(addedModifications);
            mods.addAll(removedModifications);
            return mods;
        }

        private static List<Modification> readModifications(Change state, ReadBuffer in, Serializer serializer) {
            long size = VariableLong.readPositive(in);
            List<Modification> mods = new ArrayList<>((int) size);
            for (int i = 0; i < size; i++) {
                long vid = VariableLong.readPositive(in);
                org.janusgraph.diskstorage.Entry entry = BufferUtil.readEntry(in,serializer);
                mods.add(new Modification(state,vid,entry));
            }
            return mods;
        }

    }

    public static class SecondaryFailures {

        public final boolean userLogFailure;
        public final Set<String> failedIndexes;

        private SecondaryFailures(StaticBuffer content, Serializer serializer) {
            ReadBuffer in = content.asReadBuffer();
            this.userLogFailure = !in.getBoolean();
            int size = in.getInt();
            HashSet<String> failedIndexes = new HashSet<>(size);
            for (int i = 0; i < size; i++) {
                failedIndexes.add(serializer.readObjectNotNull(in,String.class));
            }
            this.failedIndexes = Collections.unmodifiableSet(failedIndexes);
        }

    }

    public static class Modification {

        public final Change state;
        public final long outVertexId;
        public final org.janusgraph.diskstorage.Entry relationEntry;

        private Modification(Change state, long outVertexId, org.janusgraph.diskstorage.Entry relationEntry) {
            this.state = state;
            this.outVertexId = outVertexId;
            this.relationEntry = relationEntry;
        }
    }

}
