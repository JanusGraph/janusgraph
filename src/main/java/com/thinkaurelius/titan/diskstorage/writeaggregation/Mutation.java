package com.thinkaurelius.titan.diskstorage.writeaggregation;

import com.thinkaurelius.titan.diskstorage.Entry;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class Mutation {

    private List<Entry> additions;

    private List<ByteBuffer> deletions;

    public Mutation(List<Entry> additions, List<ByteBuffer> deletions) {
        this.additions=additions;
        this.deletions=deletions;
    }
    
    public List<Entry> getAdditions() {
        return additions;
    }

    public List<ByteBuffer> getDeletions() {
        return deletions;
    }
    
    public void merge(Mutation m) {
        if (additions==null) additions = m.additions;
        else additions.addAll(m.additions);
        
        if (deletions==null) deletions = m.deletions;
        else deletions.addAll(m.deletions);
    }

}
