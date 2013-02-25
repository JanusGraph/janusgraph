package com.thinkaurelius.titan.diskstorage.indexing;

import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface IndexInformation {

    public boolean supports(Class<?> dataType, Relation relation);

    public boolean supports(Class<?> dataType);

}
