package org.janusgraph.diskstorage.keycolumnvalue.cache;

import org.janusgraph.diskstorage.StaticBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface CachableStaticBuffer extends StaticBuffer {

    public int getCacheMarker();

}
