package com.thinkaurelius.titan.diskstorage.log.kcvs;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ExternalPersistor {

    public void add(StaticBuffer key, Entry cell);

}
