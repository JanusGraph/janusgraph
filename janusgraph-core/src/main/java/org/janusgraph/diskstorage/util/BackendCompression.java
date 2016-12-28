package org.janusgraph.diskstorage.util;

import org.janusgraph.diskstorage.StaticBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface BackendCompression {

    public StaticBuffer compress(StaticBuffer value);

    public StaticBuffer decompress(StaticBuffer value);

    public static final BackendCompression NO_COMPRESSION = new BackendCompression() {
        @Override
        public StaticBuffer compress(StaticBuffer value) {
            return value;
        }

        @Override
        public StaticBuffer decompress(StaticBuffer value) {
            return value;
        }
    };

}
