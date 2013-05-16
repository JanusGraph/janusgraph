package com.thinkaurelius.titan.graphdb.query.keycondition;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface Relation {

    public boolean isValidCondition(Object condition);

    public boolean isValidDataType(Class<?> clazz);

    public boolean satisfiesCondition(Object value, Object condition);

}
