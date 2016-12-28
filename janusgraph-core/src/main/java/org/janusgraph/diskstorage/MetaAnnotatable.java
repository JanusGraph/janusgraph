package org.janusgraph.diskstorage;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface MetaAnnotatable {

    public Object setMetaData(EntryMetaData key, Object value);

}
