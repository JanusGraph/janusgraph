package com.thinkaurelius.titan.diskstorage.log.kcvs;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.log.AbstractMessage;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KCVSMessage extends AbstractMessage {

    public KCVSMessage(StaticBuffer payload, long timestamp, String senderId) {
        super(payload, timestamp, senderId);
    }
}
