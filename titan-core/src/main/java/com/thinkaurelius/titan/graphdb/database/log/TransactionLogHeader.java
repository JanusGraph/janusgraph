package com.thinkaurelius.titan.graphdb.database.log;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.management.LogTxStatus;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TransactionLogHeader {

    private final long transactionId;
    private final long txTimestamp;
    private LogTxStatus status;

    public TransactionLogHeader(long transactionId, long txTimestamp, LogTxStatus status) {
        Preconditions.checkArgument(transactionId>0);
        Preconditions.checkArgument(txTimestamp>0);
        Preconditions.checkArgument(status!=null);
        this.transactionId = transactionId;
        this.txTimestamp = txTimestamp;
        this.status = status;
    }


    public long getId() {
        return transactionId;
    }

    public long getTimestamp(TimeUnit unit) {
        return unit.convert(txTimestamp,TimeUnit.MICROSECONDS);
    }

    public LogTxStatus getStatus() {
        return status;
    }

    public StaticBuffer getLogKey() {
        ByteBuffer b = ByteBuffer.allocate(8);
        b.order(ByteOrder.LITTLE_ENDIAN);
        b.putLong(transactionId);
        return new StaticArrayBuffer(b.array());
    }

    public void setStatus(LogTxStatus status) {
        Preconditions.checkArgument(status!=null);
        this.status=status;
    }

    public DataOutput serializeHeader(Serializer serializer, int capacity) {
        DataOutput out = serializer.getDataOutput(capacity);
        out.putLong(txTimestamp);
        VariableLong.writePositive(out, transactionId);
        out.writeObjectNotNull(status);
        return out;
    }

    public static Entry parse(StaticBuffer buffer, Serializer serializer) {
        ReadBuffer read = buffer.asReadBuffer();
        long txTimestamp = read.getLong();
        TransactionLogHeader header = new TransactionLogHeader(VariableLong.readPositive(read),
                txTimestamp,serializer.readObjectNotNull(read,LogTxStatus.class));
        if (read.hasRemaining()) {
            StaticBuffer content = read.subrange(read.getPosition(),read.length()-read.getPosition());
            return new Entry(header,content);
        } else {
            return new Entry(header,null);
        }
    }

    public static class Entry {

        private final TransactionLogHeader header;
        private final StaticBuffer content;

        public Entry(TransactionLogHeader header, StaticBuffer content) {
            Preconditions.checkArgument(header!=null);
            Preconditions.checkArgument(content==null || content.length()>0);
            this.header = header;
            this.content = content;
        }

        public TransactionLogHeader getHeader() {
            return header;
        }

        public boolean hasContent() {
            return content!=null;
        }

        public StaticBuffer getContent() {
            Preconditions.checkState(hasContent(),"Does not have any content");
            return content;
        }
    }

}
