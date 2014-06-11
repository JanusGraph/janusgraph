package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.BaseTransaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Element;

/**
 * The TitanManagement interface provides methods to define, update, and inspect the schema of a Titan graph.
 * It wraps a {@link TitanTransaction} and therefore copies many of its methods as they relate to schema inspection
 * and definition.
 * <p/>
 * TitanManagement behaves like a transaction in that it opens a transactional scope for reading the schema and making
 * changes to it. As such, it needs to be explicitly closed via its {@link #commit()} or {@link #rollback()} methods.
 * A TitanManagement transaction is opened on a graph via {@link com.thinkaurelius.titan.core.TitanGraph#getManagementSystem()}.
 * <p/>
 * TitanManagement provides methods to:
 * <ul>
 *     <li>Schema Types: View, update, and create vertex labels, edge labels, and property keys</li>
 *     <li>Relation Type Index: View and create vertex-centric indexes on edge labels and property keys</li>
 *     <li>Graph Index: View and create graph-wide indexes for efficient element retrieval</li>
 *     <li>Consistency Management: Set the consistency level of individual schema elements</li>
 * </ul>
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanManagement extends TitanConfiguration {

    /*
    ##################### RELATION TYPE INDEX ##########################
     */

    /**
     * Identical to {@link #createEdgeIndex(com.thinkaurelius.titan.core.EdgeLabel, String, com.tinkerpop.blueprints.Direction, com.thinkaurelius.titan.core.Order, com.thinkaurelius.titan.core.RelationType...)}
     * with default sort order {@link Order#ASC}.
     *
     * @param label
     * @param name
     * @param direction
     * @param sortKeys
     * @return the created {@link RelationTypeIndex}
     */
    public RelationTypeIndex createEdgeIndex(EdgeLabel label, String name, Direction direction, RelationType... sortKeys);

    /**
     * Creates a {@link RelationTypeIndex} for the provided edge label. That means, that all edges of that label will be
     * indexed according to this index definition which will speed up certain vertex-centric queries.
     * <p/>
     * An indexed is defined by its name, the direction in which the index should be created (can be restricted to one
     * direction or both), the sort order and - most importantly - the sort keys which define the index key.
     *
     * @param label
     * @param name
     * @param direction
     * @param sortOrder
     * @param sortKeys
     * @return the created {@link RelationTypeIndex}
     */
    public RelationTypeIndex createEdgeIndex(EdgeLabel label, String name, Direction direction, Order sortOrder, RelationType... sortKeys);

    /**
     * Identical to {@link #createPropertyIndex(com.thinkaurelius.titan.core.PropertyKey, String, com.thinkaurelius.titan.core.Order, com.thinkaurelius.titan.core.RelationType...)}
     * with default sort order {@link Order#ASC}.
     *
     * @param key
     * @param name
     * @param sortKeys
     * @return the created {@link RelationTypeIndex}
     */
    public RelationTypeIndex createPropertyIndex(PropertyKey key, String name, RelationType... sortKeys);

    /**
     * Creates a {@link RelationTypeIndex} for the provided property key. That means, that all properties of that key will be
     * indexed according to this index definition which will speed up certain vertex-centric queries.
     * <p/>
     * An indexed is defined by its name, the sort order and - most importantly - the sort keys which define the index key.
     *
     * @param key
     * @param name
     * @param sortOrder
     * @param sortKeys
     * @return the created {@link RelationTypeIndex}
     */
    public RelationTypeIndex createPropertyIndex(PropertyKey key, String name, Order sortOrder, RelationType... sortKeys);

    /**
     * Whether a {@link RelationTypeIndex} with the given name has been defined for the provided {@link RelationType}
     * @param type
     * @param name
     * @return
     */
    public boolean containsRelationIndex(RelationType type, String name);

    /**
     * Returns the {@link RelationTypeIndex} with the given name for the provided {@link RelationType} or null
     * if it does not exist
     *
     * @param type
     * @param name
     * @return
     */
    public RelationTypeIndex getRelationIndex(RelationType type, String name);

    /**
     * Returns an {@link Iterable} over all {@link RelationTypeIndex}es defined for the provided {@link RelationType}
     * @param type
     * @return
     */
    public Iterable<RelationTypeIndex> getRelationIndexes(RelationType type);

    /*
    ##################### GRAPH INDEX ##########################
     */


    /**
     * Whether the graph has a graph index defined with the given name.
     *
     * @param name
     * @return
     */
    public boolean containsGraphIndex(String name);

    /**
     * Returns the graph index with the given name or null if it does not exist
     *
     * @param name
     * @return
     */
    public TitanGraphIndex getGraphIndex(String name);

    /**
     * Returns all graph indexes that index the given element type.
     *
     * @param elementType
     * @return
     */
    public Iterable<TitanGraphIndex> getGraphIndexes(final Class<? extends Element> elementType);

    /**
     * Returns an {@link IndexBuilder} to add a graph index to this Titan graph. The index to-be-created
     * has the provided name and indexes elements of the given type.
     *
     * @param indexName
     * @param elementType
     * @return
     */
    public IndexBuilder buildIndex(String indexName, Class<? extends Element> elementType);


    public void addIndexKey(final TitanGraphIndex index, final PropertyKey key, Parameter... parameters);

    /**
     * Builder for {@link TitanGraphIndex}. Allows for the configuration of a graph index prior to its construction.
     */
    public interface IndexBuilder {

        /**
         * Adds the given key to the composite key of this index
         *
         * @param key
         * @return this IndexBuilder
         */
        public IndexBuilder indexKey(PropertyKey key);

        /**
         * Adds the given key and associated parameters to the composite key of this index
         * @param key
         * @param parameters
         * @return this IndexBuilder
         */
        public IndexBuilder indexKey(PropertyKey key, Parameter... parameters);

        /**
         * Restricts this index to only those elements that have the provided schemaType. If this graph index indexes
         * vertices, then the argument is expected to be a vertex label and only vertices with that label will be indexed.
         * Likewise, for edges and properties only those with the matching relation type will be indexed.
         *
         * @param schemaType
         * @return this IndexBuilder
         */
        public IndexBuilder indexOnly(TitanSchemaType schemaType);

        /**
         * Makes this a unique index for the configured element type,
         * i.e. an index key can be associated with at most one element in the graph.
         *
         * @return this IndexBuilder
         */
        public IndexBuilder unique();

        /**
         * Builds an internal index according to the specification
         *
         * @return the created internal {@link TitanGraphIndex}
         */
        public TitanGraphIndex buildInternalIndex();

        /**
         * Builds an external index according to the specification against the backend index with the given name (i.e.
         * the name under which that index is configured in the graph configuration)
         *
         * @param backingIndex the name of the external index
         * @return the created internal {@link TitanGraphIndex}
         */
        public TitanGraphIndex buildExternalIndex(String backingIndex);

    }

    /*
    ##################### CONSISTENCY SETTING ##########################
     */

    /**
     * Retrieves the consistency modifier for the given {@link TitanSchemaElement}. If none has been explicitly
     * defined, {@link ConsistencyModifier#DEFAULT} is returned.
     *
     * @param element
     * @return
     */
    public ConsistencyModifier getConsistency(TitanSchemaElement element);

    /**
     * Sets the consistency modifier for the given {@link TitanSchemaElement}. Note, that only {@link RelationType}s
     * and internal graph indexes allow changing of the consistency level.
     *
     * @param element
     * @param consistency
     */
    public void setConsistency(TitanSchemaElement element, ConsistencyModifier consistency);


    /*
    ##################### PROXY FOR TITANTRANSACTION ##########################
     */

    /**
     * Identical to {@link TitanTransaction#containsRelationType(String)}
     *
     * @param name
     * @return
     */
    public boolean containsRelationType(String name);

    /**
     * Identical to {@link TitanTransaction#getRelationType(String)}
     *
     * @param name
     * @return
     */
    public RelationType getRelationType(String name);

    /**
     * Identical to {@link TitanTransaction#getPropertyKey(String)}
     *
     * @param name
     * @return
     */
    public PropertyKey getPropertyKey(String name);

    /**
     * Identical to {@link TitanTransaction#getEdgeLabel(String)}
     *
     * @param name
     * @return
     */
    public EdgeLabel getEdgeLabel(String name);

    /**
     * Identical to {@link TitanTransaction#makePropertyKey(String)}
     *
     * @param name
     * @return
     */
    public PropertyKeyMaker makePropertyKey(String name);

    /**
     * Identical to {@link TitanTransaction#makeEdgeLabel(String)}
     *
     * @param name
     * @return
     */
    public EdgeLabelMaker makeEdgeLabel(String name);

    /**
     * Returns an iterable over all defined types that have the given clazz (either {@link EdgeLabel} which returns all labels,
     * {@link PropertyKey} which returns all keys, or {@link RelationType} which returns all types).
     *
     * @param clazz {@link RelationType} or sub-interface
     * @param <T>
     * @return Iterable over all types for the given category (label, key, or both)
     */
    public <T extends RelationType> Iterable<T> getRelationTypes(Class<T> clazz);

    /**
     * Identical to {@link TitanTransaction#containsVertexLabel(String)}
     *
     * @param name
     * @return
     */
    public boolean containsVertexLabel(String name);

    /**
     * Identical to {@link TitanTransaction#getVertexLabel(String)}
     *
     * @param name
     * @return
     */
    public VertexLabel getVertexLabel(String name);

    /**
     * Identical to {@link TitanTransaction#makeVertexLabel(String)}
     *
     * @param name
     * @return
     */
    public VertexLabelMaker makeVertexLabel(String name);

    /**
     * Returns an {@link Iterable} over all defined {@link VertexLabel}s.
     *
     * @return
     */
    public Iterable<VertexLabel> getVertexLabels();

    /**
     * Whether this management transaction is open or has been closed (i.e. committed or rolled-back)
     * @return
     */
    public boolean isOpen();

    /**
     * Commits this management transaction and persists all schema changes. Closes this transaction.
     * @see com.thinkaurelius.titan.core.TitanTransaction#commit()
     */
    public void commit();

    /**
     * Closes this management transaction and discards all changes.
     * @see com.thinkaurelius.titan.core.TitanTransaction#rollback()
     */
    public void rollback();

}
