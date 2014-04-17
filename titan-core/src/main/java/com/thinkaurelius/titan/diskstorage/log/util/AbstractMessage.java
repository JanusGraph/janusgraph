package com.thinkaurelius.titan.diskstorage.log.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.log.Message;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Abstract implementation of {@link com.thinkaurelius.titan.diskstorage.log.Message} which exposes the timestamp, sender, and payload
 * of a message.
 * Particular {@link com.thinkaurelius.titan.diskstorage.log.Log} implementations can extend this class.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractMessage implements Message {

    private static final int MAX_PAYLOAD_STR_LENGTH = 400;

    private final StaticBuffer content;
    private final long timestampMicro;
    private final String senderId;

    protected AbstractMessage(StaticBuffer content, long timestampMicro, String senderId) {
        Preconditions.checkArgument(content !=null && senderId!=null);
        this.content = content;
        this.timestampMicro = timestampMicro;
        this.senderId = senderId;
    }

    @Override
    public String getSenderId() {
        return senderId;
    }

    public long getTimestampMicro() {
        return timestampMicro;
    }

    @Override
    public long getTimestamp(TimeUnit unit) {
        return unit.convert(timestampMicro,TimeUnit.MICROSECONDS);
    }

    @Override
    public StaticBuffer getContent() {
        return content;
    }

    @Override
    public String toString() {
        String paystr = content.toString();
        if (paystr.length()>MAX_PAYLOAD_STR_LENGTH) paystr=paystr.substring(0,MAX_PAYLOAD_STR_LENGTH) + "...";
        return "Message@" + Long.toString(timestampMicro) + ":" + senderId + "=" + paystr;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(content).append(timestampMicro).append(senderId).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null || !getClass().isInstance(other)) return false;
        AbstractMessage msg = (AbstractMessage)other;
        return timestampMicro ==msg.timestampMicro && senderId.equals(msg.senderId) && content.equals(msg.content);

    }



}
