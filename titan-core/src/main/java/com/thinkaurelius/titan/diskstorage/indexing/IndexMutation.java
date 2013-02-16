package com.thinkaurelius.titan.diskstorage.indexing;

import com.thinkaurelius.titan.diskstorage.Mutation;

import java.util.List;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class IndexMutation extends Mutation<IndexEntry,String> {

    public IndexMutation(List<IndexEntry> additions, List<String> deletions) {
        super(additions, deletions);
    }

    public IndexMutation() {
        super();
    }

}
