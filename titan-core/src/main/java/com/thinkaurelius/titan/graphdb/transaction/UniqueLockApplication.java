package com.thinkaurelius.titan.graphdb.transaction;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Used to identify a lock for relations with a uniqueness property so that a corresponding lock can be acquired during
 * creation to ensure uniqueness.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class UniqueLockApplication {

    private final TitanVertex start;
    private final TitanType type;
    private final Object end;
    private final int hashcode;

    public UniqueLockApplication(final TitanVertex start, TitanType type, final Object end) {
        Preconditions.checkNotNull(start);
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(end);
        Preconditions.checkArgument(type.isUnique(Direction.OUT) || type.isUnique(Direction.IN));
        this.type=type;
        if (type.isUnique(Direction.OUT)) this.start=start;
        else this.start=null;
        if (type.isUnique(Direction.IN)) this.end=end;
        else this.end=null;
        hashcode = new HashCodeBuilder().append(type).append(start).append(end).toHashCode();
    }


    @Override
    public int hashCode() {
        return hashcode;
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        UniqueLockApplication oth = (UniqueLockApplication)other;
        if (!type.equals(oth.type)) return false;
        if (start==null) {
            if (oth.start!=null) return false;
        } else if (!start.equals(oth.start)) return false;
        if (end==null) {
            if (oth.end!=null) return false;
        } else if (!end.equals(oth.end)) return false;
        return true;
    }


    @Override
    public String toString() {
        return "("+start+"-"+type+"->"+end+")";
    }


}
