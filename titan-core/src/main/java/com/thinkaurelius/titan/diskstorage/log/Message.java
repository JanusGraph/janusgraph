package com.thinkaurelius.titan.diskstorage.log;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Message {

    public String getSenderId();

    public long getTimestamp();

//    public String getMessageId();

    public StaticBuffer getPayLoad();

}
