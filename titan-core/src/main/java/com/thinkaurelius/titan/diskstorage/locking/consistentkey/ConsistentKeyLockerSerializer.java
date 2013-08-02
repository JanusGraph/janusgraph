package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.diskstorage.util.WriteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;

/**
 * Translate locking coordinates and metadata (data keys, data columns, data
 * values, timestamps, and rids) into keys, columns, and values compatible with
 * {@link ConsistentKeyLocker} and vice-versa.
 */
public class ConsistentKeyLockerSerializer {
     
    public StaticBuffer toLockKey(StaticBuffer key, StaticBuffer column) {
        WriteBuffer b = new WriteByteBuffer(key.length() + column.length() + 4);
        b.putInt(key.length());
        WriteBufferUtil.put(b,key);
        WriteBufferUtil.put(b,column);
        return b.getStaticBuffer();
    }
    
    public StaticBuffer toLockCol(long ts, StaticBuffer rid) {
        WriteBuffer b = new WriteByteBuffer(rid.length() + 8);
        b.putLong(ts);
        WriteBufferUtil.put(b, rid);
        return b.getStaticBuffer();
    }
    
    public TimestampRid fromLockColumn(StaticBuffer lockKey) {
        ReadBuffer r = lockKey.asReadBuffer();
        int len = r.length();
        long tsNS = r.getLong();
        len -= 8;
        byte[] curRid = new byte[len];
        for (int i = 0; r.hasRemaining(); i++) {
            curRid[i] = r.getByte();
        }
        StaticBuffer rid = new StaticByteBuffer(curRid);
        return new TimestampRid(tsNS, rid);
    }
}
