package com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface CachableStaticBuffer extends StaticBuffer {

    public int getCacheMarker();

}
