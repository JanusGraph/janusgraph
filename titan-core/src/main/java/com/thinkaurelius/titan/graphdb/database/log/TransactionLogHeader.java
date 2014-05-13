package com.thinkaurelius.titan.graphdb.database.log;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.util.time.StandardTimepoint;
import com.thinkaurelius.titan.diskstorage.util.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.HashingUtil;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfiguration;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;

import java.util.EnumMap;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TransactionLogHeader {

    private final long transactionId;
    private final Timepoint txTimestamp;
    private final StaticBuffer logKey;

    public TransactionLogHeader(long transactionId, Timepoint txTimestamp) {
        this.transactionId = transactionId;
        this.txTimestamp = txTimestamp;
        Preconditions.checkArgument(this.transactionId > 0);
        Preconditions.checkNotNull(this.txTimestamp);
        logKey = HashingUtil.hashPrefixKey(HashingUtil.HashLength.SHORT, BufferUtil.getLongBuffer(transactionId));
    }


    public long getId() {
        return transactionId;
    }

    public long getTimestamp(TimeUnit unit) {
        return txTimestamp.getTimestamp(unit);
    }

    public StaticBuffer getLogKey() {
        return logKey;
    }

    public DataOutput serializeHeader(Serializer serializer, int capacity, LogTxStatus status) {
        return serializeHeader(serializer, capacity, status, null);
    }

    public DataOutput serializeHeader(Serializer serializer, int capacity, LogTxStatus status, TransactionConfiguration txConfig) {
        Preconditions.checkArgument(status!=null,"Invalid status");
        DataOutput out = serializer.getDataOutput(capacity);
        out.putLong(txTimestamp.getNativeTimestamp());
        VariableLong.writePositive(out, transactionId);
        out.writeObjectNotNull(status);
        if (txConfig!=null) {
            int metaSize = 0;
            for (LogTxMeta meta: LogTxMeta.values()) if (meta.getValue(txConfig)!=null) metaSize++;
            out.putByte(VariableLong.unsignedByte(metaSize));
            for (LogTxMeta meta : LogTxMeta.values()) {
                Object value = meta.getValue(txConfig);
                if (value!=null) {
                    out.putByte(VariableLong.unsignedByte(meta.ordinal()));
                    out.writeObjectNotNull(value);
                }
            }
        } else {
            out.putByte(VariableLong.unsignedByte(0));
        }
        return out;
    }

    public static Entry parse(StaticBuffer buffer, Serializer serializer, TimestampProvider times) {
        ReadBuffer read = buffer.asReadBuffer();
        Timepoint txTimestamp = new StandardTimepoint(read.getLong(), times);
        TransactionLogHeader header = new TransactionLogHeader(VariableLong.readPositive(read),
                txTimestamp);
        LogTxStatus status = serializer.readObjectNotNull(read,LogTxStatus.class);
        EnumMap<LogTxMeta,Object> metadata = new EnumMap<LogTxMeta, Object>(LogTxMeta.class);
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
    }

}
