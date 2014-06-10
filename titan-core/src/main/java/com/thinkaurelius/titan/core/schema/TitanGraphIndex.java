package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.PropertyKey;
import com.tinkerpop.blueprints.Element;

/**
 * A TitanGraphIndex is an index installed on the graph in order to be able to efficiently retrieve graph elements
 * by their properties.
 * A TitanGraphIndex may either be an internal or external index and is created via {@link TitanManagement#buildIndex(String, Class)}.
 * <p/>
 * This interface allows introspecting an existing graph index. Existing graph indexes can be retrieved via
 * {@link TitanManagement#getGraphIndex(String)} or {@link TitanManagement#getGraphIndexes(Class)}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanGraphIndex extends TitanSchemaElement {

    /**
     * Returns the name of the index
     * @return
     */
    public String getName();

    /**
     * Returns the name of the backing index. For internal indexes this returns a default name.
     * For external indexes, this returns the name of the configured indexing backend.
     *
     * @return
     */
    public String getBackingIndex();

    /**
     * Returns which element type is being indexed by this index (vertex, edge, or property)
     *
     * @return
     */
    public Class<? extends Element> getIndexedElement();

    /**
     * Returns the indexed keys of this index. If the returned array contains more than one element, its a
     * composite index.
     *
     * @return
     */
    public PropertyKey[] getFieldKeys();

    /**
     * Returns the parameters associated with an indexed key of this index. Parameters modify the indexing
     * behavior of the underlying indexing backend.
     *
     * @param key
     * @return
     */
    public Parameter[] getParametersFor(PropertyKey key);

    /**
     * Whether this is a unique index, i.e. values are uniquely associated with at most one element in the graph (for
     * a particular type)
     *
     * @return
     */
    public boolean isUnique();

}
