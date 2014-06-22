package com.thinkaurelius.titan.graphdb.transaction.lock;

import com.thinkaurelius.titan.graphdb.types.CompositeIndexType;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IndexLockTuple extends LockTuple {

    private final CompositeIndexType index;

    public IndexLockTuple(CompositeIndexType index, Object... tuple) {
        super(tuple);
        this.index=index;
    }

    public CompositeIndexType getIndex() {
        return index;
    }

    @Override
    public int hashCode() {
        return super.hashCode()*10043 + Long.valueOf(index.getID()).hashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null || !(oth instanceof IndexLockTuple)) return false;
        return super.equals(oth) && ((IndexLockTuple)oth).index.getID()==index.getID();
    }

    @Override
    public String toString() {
        return super.toString()+":"+index.getID();
    }


}
