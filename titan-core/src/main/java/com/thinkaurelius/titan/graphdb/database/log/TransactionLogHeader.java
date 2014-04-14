package com.thinkaurelius.titan.graphdb.database.log;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
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
    private final long txTimestamp;
    private final TimeUnit timeUnit;
    private LogTxStatus status;

    public TransactionLogHeader(long transactionId, long txTimestamp, TimeUnit timeUnit, LogTxStatus status) {
        Preconditions.checkArgument(transactionId>0);
        Preconditions.checkArgument(txTimestamp>0);
        Preconditions.checkArgument(status!=null && timeUnit!=null);
        this.transactionId = transactionId;
        this.txTimestamp = txTimestamp;
        this.status = status;
        this.timeUnit = timeUnit;
    }


    public long getId() {
        return transactionId;
    }

    public long getTimestamp(TimeUnit unit) {
        return unit.convert(txTimestamp,timeUnit);
    }

    public LogTxStatus getStatus() {
        return status;
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

    public static Entry parse(StaticBuffer buffer, Serializer serializer, TimeUnit unit) {
        ReadBuffer read = buffer.asReadBuffer();
        long txTimestamp = read.getLong();
        TransactionLogHeader header = new TransactionLogHeader(VariableLong.readPositive(read),
                txTimestamp,unit,
                serializer.readObjectNotNull(read,LogTxStatus.class));
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
