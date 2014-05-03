package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.hash.HashCode;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class HashingUtil {

    public enum HashLength {
        SHORT, LONG;

        public int length() {
            switch (this) {
                case SHORT: return 4;
                case LONG: return 8;
                default: throw new AssertionError("Unknown hash type: " + this);
            }
        }
    }

    private static final StaticBuffer.Factory<HashCode> SHORT_HASH_FACTORY = new StaticBuffer.Factory<HashCode>() {
        @Override
        public HashCode get(byte[] array, int offset, int limit) {
            return HashUtility.SHORT.get().hashBytes(array, offset, limit);
        }
    };

    private static final StaticBuffer.Factory<HashCode> LONG_HASH_FACTORY = new StaticBuffer.Factory<HashCode>() {
        @Override
        public HashCode get(byte[] array, int offset, int limit) {
            return HashUtility.LONG.get().hashBytes(array,offset,limit);
        }
    };

    public static final StaticBuffer hashPrefixKey(final HashLength hashPrefixLen, final StaticBuffer key) {
        final int prefixLen = hashPrefixLen.length();
        final StaticBuffer.Factory<HashCode> hashFactory;
        switch (hashPrefixLen) {
            case SHORT:
                hashFactory = SHORT_HASH_FACTORY;
                break;
            case LONG:
                hashFactory = LONG_HASH_FACTORY;
                break;
            default: throw new IllegalArgumentException("Unknown hash prefix: " + hashPrefixLen);
        }

        HashCode hashcode = key.as(hashFactory);
        WriteByteBuffer newKey = new WriteByteBuffer(prefixLen+key.length());
        assert prefixLen==4 || prefixLen==8;
        if (prefixLen==4) newKey.putInt(hashcode.asInt());
        else newKey.putLong(hashcode.asLong());
        newKey.putBytes(key);
        return newKey.getStaticBuffer();
    }

    public static final StaticBuffer getKey(final HashLength hashPrefixLen, StaticBuffer hasPrefixedKey) {
        return hasPrefixedKey.subrange(hashPrefixLen.length(), hasPrefixedKey.length() - hashPrefixLen.length());
    }



}
