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

    public boolean hasAdditions() {
        return additions!=null && !additions.isEmpty();
    }

    public boolean hasDeletions() {
        return deletions!=null && !deletions.isEmpty();
    }

    public List<Entry> getAdditions() {
        return additions;
    }

    public List<ByteBuffer> getDeletions() {
        return deletions;
    }
    
    public void merge(Mutation m) {
    	
    	if (null == m) {
    		return;
    	}
    	
    	if (null != m.additions) {
    		if (null == additions) additions = m.additions;
    		else additions.addAll(m.additions);
    	}
    	
        if (null != m.deletions) {
        	if (null == deletions) deletions = m.deletions;
        	else deletions.addAll(m.deletions);
        }
    }

}
