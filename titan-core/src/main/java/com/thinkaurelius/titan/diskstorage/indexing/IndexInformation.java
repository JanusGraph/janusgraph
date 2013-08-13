package com.thinkaurelius.titan.diskstorage.indexing;

import com.thinkaurelius.titan.graphdb.query.TitanPredicate;

/**
 * An IndexInformation gives basic information on what a particular {@link IndexProvider} supports.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface IndexInformation {

    /**
     * Whether the index supports executing queries with the given relation and data type.
     * @param dataType
     * @param titanPredicate
     * @return
     */
    public boolean supports(Class<?> dataType, TitanPredicate titanPredicate);

    /**
     * Whether the index supports the given data type
     * @param dataType
     * @return
     */
    public boolean supports(Class<?> dataType);

}
