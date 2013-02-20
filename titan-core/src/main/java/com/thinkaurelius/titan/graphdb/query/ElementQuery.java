package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyCondition;
import org.apache.commons.lang.StringUtils;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class ElementQuery {

    public enum Type { VERTEX, EDGE }

    private final KeyCondition<InternalType> condition;
    private final Type type;

    public ElementQuery(Type type, KeyCondition<InternalType> condition) {
        Preconditions.checkNotNull(condition);
        Preconditions.checkNotNull(type);
        this.condition = condition;
        this.type=type;
    }

    @Override
    public int hashCode() {
        return condition.hashCode()*9676463 + type.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        ElementQuery oth = (ElementQuery)other;
        return type==oth.type && condition.equals(oth.condition);
    }

    @Override
    public String toString() {
        return "["+condition.toString()+"]:"+type.toString();
    }

}
