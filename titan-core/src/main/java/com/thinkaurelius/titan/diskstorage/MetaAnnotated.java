package com.thinkaurelius.titan.diskstorage;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface MetaAnnotated {

    /**
     * Returns true if this entry has associated meta data
     * @return
     */
    public boolean hasMetaData();

    /**
     * Returns all meta data associated with this entry
     * @return
     */
    public Map<EntryMetaData,Object> getMetaData();

}
