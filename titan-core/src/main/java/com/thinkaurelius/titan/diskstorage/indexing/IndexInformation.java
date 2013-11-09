package com.thinkaurelius.titan.diskstorage.indexing;

import com.thinkaurelius.titan.graphdb.query.TitanPredicate;

/**
 * An IndexInformation gives basic information on what a particular {@link IndexProvider} supports.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface IndexInformation {

    /**
     * Whether the index supports executing queries with the given predicate against a key with the given information
     * @param information
     * @param titanPredicate
     * @return
     */
    public boolean supports(KeyInformation information, TitanPredicate titanPredicate);

    /**
     * Whether the index supports indexing a key with the given information
     * @param information
     * @return
     */
    public boolean supports(KeyInformation information);

}
