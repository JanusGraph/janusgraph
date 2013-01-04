package com.thinkaurelius.titan.diskstorage.indexing;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface TextIndex {

    public void add(String documentId, String field, String value);

}
