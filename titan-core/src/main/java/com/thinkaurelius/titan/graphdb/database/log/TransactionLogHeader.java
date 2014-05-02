package com.thinkaurelius.titan.graphdb.database.log;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.indexing.HashPrefixKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.management.LogTxStatus;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;

import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TransactionLogHeader {

    private final long transactionId;
    private final Timepoint txTimestamp;
    private final TimeUnit backendTimeUnit;
    private final StaticBuffer logKey;

    public TransactionLogHeader(long transactionId, Timepoint txTimestamp, TimeUnit backendTimeUnit) {
        this.transactionId = transactionId;
        this.txTimestamp = txTimestamp;
        this.backendTimeUnit = backendTimeUnit;
        Preconditions.checkArgument(this.transactionId > 0);
        Preconditions.checkNotNull(this.txTimestamp);
        logKey = HashPrefixKeyColumnValueStore.prefixKey(HashPrefixKeyColumnValueStore.HashLength.SHORT,BufferUtil.getLongBuffer(transactionId));
    }


    public long getId() {
        return transactionId;
    }

    public long getTimestamp(TimeUnit unit) {
        return txTimestamp.getTime(unit);
    }

    public StaticBuffer getLogKey() {
        return logKey;
    }

    public DataOutput serializeHeader(Serializer serializer, int capacity, LogTxStatus status) {
        Preconditions.checkArgument(status!=null,"Invalid status");
        DataOutput out = serializer.getDataOutput(capacity);
        out.putLong(txTimestamp.getTime(backendTimeUnit));
        VariableLong.writePositive(out, transactionId);
        out.writeObjectNotNull(status);
        return out;
    }

    public static Entry parse(StaticBuffer buffer, Serializer serializer, TimeUnit backendTimeUnit) {
        ReadBuffer read = buffer.asReadBuffer();
        Timepoint txTimestamp = new Timepoint(read.getLong(), backendTimeUnit);
        TransactionLogHeader header = new TransactionLogHeader(VariableLong.readPositive(read),
                txTimestamp, backendTimeUnit);
        LogTxStatus status = serializer.readObjectNotNull(read,LogTxStatus.class);
        if (read.hasRemaining()) {
            StaticBuffer content = read.subrange(read.getPosition(),read.length()-read.getPosition());
            return new Entry(header,content, status);
        } else {
            return new Entry(header,null, status);
        }
    }

    public static class Entry {

        private final TransactionLogHeader header;
        private final StaticBuffer content;
        private final LogTxStatus status;

        public Entry(TransactionLogHeader header, StaticBuffer content, LogTxStatus status) {
            Preconditions.checkNotNull(status);
            Preconditions.checkArgument(header!=null);
            Preconditions.checkArgument(content==null || content.length()>0);
            this.header = header;
            this.content = content;
            this.status = status;
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

        public StaticBuffer getContent() {
            Preconditions.checkState(hasContent(),"Does not have any content");
            return content;
        }
    }

}
