package com.thinkaurelius.titan.diskstorage.log;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractMessage implements Message {

    private static final int MAX_PAYLOAD_STR_LENGTH = 400;

    private final StaticBuffer payload;
    private final long timestamp;
    private final String senderId;

    public AbstractMessage(StaticBuffer payload, long timestamp, String senderId) {
        Preconditions.checkArgument(payload!=null && senderId!=null);
        this.payload = payload;
        this.timestamp = timestamp;
        this.senderId = senderId;
    }

    @Override
    public String getSenderId() {
        return senderId;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public StaticBuffer getPayLoad() {
        return payload;
    }

    @Override
    public String toString() {
        String paystr = payload.toString();
        if (paystr.length()>MAX_PAYLOAD_STR_LENGTH) paystr=paystr.substring(0,MAX_PAYLOAD_STR_LENGTH) + "...";
        return "Message@" + Long.toString(timestamp) + ":" + senderId + "=" + paystr;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(payload).append(timestamp).append(senderId).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null || !getClass().isInstance(other)) return false;
        AbstractMessage msg = (AbstractMessage)other;
        return timestamp==msg.timestamp && senderId.equals(msg.senderId) && payload.equals(msg.payload);

    }



}
