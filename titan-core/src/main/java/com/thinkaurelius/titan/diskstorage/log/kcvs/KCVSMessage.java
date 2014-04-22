package com.thinkaurelius.titan.diskstorage.log.kcvs;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.log.util.AbstractMessage;

/**
 * Implementation of {@link AbstractMessage} for {@link KCVSLog}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KCVSMessage extends AbstractMessage {

    public KCVSMessage(StaticBuffer payload, long timestampMirco, String senderId) {
        super(payload, timestampMirco, senderId);
    }
}
