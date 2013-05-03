package com.thinkaurelius.titan.diskstorage.indexing;

import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;

/**
 * An IndexInformation gives basic information on what a particular {@link IndexProvider} supports.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface IndexInformation {

    /**
     * Whether the index supports executing queries with the given relation and data type.
     * @param dataType
     * @param relation
     * @return
     */
    public boolean supports(Class<?> dataType, Relation relation);

    /**
     * Whether the index supports the given data type
     * @param dataType
     * @return
     */
    public boolean supports(Class<?> dataType);

}
