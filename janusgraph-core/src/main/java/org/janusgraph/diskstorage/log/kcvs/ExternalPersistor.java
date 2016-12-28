package org.janusgraph.diskstorage.log.kcvs;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ExternalPersistor {

    public void add(StaticBuffer key, Entry cell);

}
