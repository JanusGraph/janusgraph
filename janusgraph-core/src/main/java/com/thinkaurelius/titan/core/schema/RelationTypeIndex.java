package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.RelationType;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * A RelationTypeIndex is an index installed on a {@link RelationType} to speed up vertex-centric indexes for that type.
 * A RelationTypeIndex is created via
 * {@link TitanManagement#buildEdgeIndex(com.thinkaurelius.titan.core.EdgeLabel, String, com.tinkerpop.gremlin.structure.Direction, com.thinkaurelius.titan.graphdb.internal.Order, com.thinkaurelius.titan.core.RelationType...)}
 * for edge labels and
 * {@link TitanManagement#buildPropertyIndex(com.thinkaurelius.titan.core.PropertyKey, String, com.thinkaurelius.titan.graphdb.internal.Order, com.thinkaurelius.titan.core.RelationType...)}
 * for property keys.
 * <p/>
 * This interface allows the inspection of already defined RelationTypeIndex'es. An existing index on a RelationType
 * can be retrieved via {@link TitanManagement#getRelationIndex(com.thinkaurelius.titan.core.RelationType, String)}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface RelationTypeIndex extends TitanIndex {

    /**
     * Returns the {@link RelationType} on which this index is installed.
     *
     * @return
     */
    public RelationType getType();

    /**
     * Returns the sort order of this index. Index entries are sorted in this order and queries
     * which use this sort order will be faster.
     *
     * @return
     */
    public Order getSortOrder();

    /**
     * Returns the (composite) sort key for this index. The composite sort key is an ordered list of {@link RelationType}s
     *
     * @return
     */
    public RelationType[] getSortKey();

    /**
     * Returns the direction on which this index is installed. An index may cover only one or both directions.
     *
     * @return
     */
    public Direction getDirection();

    /**
     * Returns the status of this index
     *
     * @return
     */
    public SchemaStatus getIndexStatus();


}
