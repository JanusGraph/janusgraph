package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.PropertyKey;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * A TitanGraphIndex is an index installed on the graph in order to be able to efficiently retrieve graph elements
 * by their properties.
 * A TitanGraphIndex may either be a composite or mixed index and is created via {@link TitanManagement#buildIndex(String, Class)}.
 * <p/>
 * This interface allows introspecting an existing graph index. Existing graph indexes can be retrieved via
 * {@link TitanManagement#getGraphIndex(String)} or {@link TitanManagement#getGraphIndexes(Class)}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanGraphIndex extends TitanIndex {

    /**
     * Returns the name of the index
     * @return
     */
    public String name();

    /**
     * Returns the name of the backing index. For composite indexes this returns a default name.
     * For mixed indexes, this returns the name of the configured indexing backend.
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

    /**
     * Returns the status of this index with respect to the provided {@link PropertyKey}.
     * For composite indexes, the key is ignored and the status of the index as a whole is returned.
     * For mixed indexes, the status of that particular key within the index is returned.
     *
     * @return
     */
    public SchemaStatus getIndexStatus(PropertyKey key);

    /**
     * Whether this is a composite index
     * @return
     */
    public boolean isCompositeIndex();

    /**
     * Whether this is a mixed index
     * @return
     */
    public boolean isMixedIndex();


}
