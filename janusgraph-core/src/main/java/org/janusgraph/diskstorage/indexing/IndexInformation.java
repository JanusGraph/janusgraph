package org.janusgraph.diskstorage.indexing;

import org.janusgraph.graphdb.query.JanusGraphPredicate;

/**
 * An IndexInformation gives basic information on what a particular {@link IndexProvider} supports.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface IndexInformation {

    /**
     * Whether the index supports executing queries with the given predicate against a key with the given information
     * @param information
     * @param janusgraphPredicate
     * @return
     */
    public boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate);

    /**
     * Whether the index supports indexing a key with the given information
     * @param information
     * @return
     */
    public boolean supports(KeyInformation information);


    /**
     * Adjusts the name of the key so that it is a valid field name that can be used in the index.
     * JanusGraph stores this information and will use the returned name in all interactions with the index.
     * <p/>
     * Note, that mapped field names (either configured on a per key basis or through a global configuration)
     * are not adjusted and handed to the index verbatim.
     *
     * @param key
     * @param information
     * @return
     */
    public String mapKey2Field(String key, KeyInformation information);

    /**
     * The features of this index
     * @return
     */
    public IndexFeatures getFeatures();

}
