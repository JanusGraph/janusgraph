package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * The TitanManagement interface provides methods to define, update, and inspect the schema of a Titan graph.
 * It wraps a {@link TitanTransaction} and therefore copies many of its methods as they relate to schema inspection
 * and definition.
 * <p/>
 * TitanManagement behaves like a transaction in that it opens a transactional scope for reading the schema and making
 * changes to it. As such, it needs to be explicitly closed via its {@link #commit()} or {@link #rollback()} methods.
 * A TitanManagement transaction is opened on a graph via {@link com.thinkaurelius.titan.core.TitanGraph#openManagement()}.
 * <p/>
 * TitanManagement provides methods to:
 * <ul>
 * <li>Schema Types: View, update, and create vertex labels, edge labels, and property keys</li>
 * <li>Relation Type Index: View and create vertex-centric indexes on edge labels and property keys</li>
 * <li>Graph Index: View and create graph-wide indexes for efficient element retrieval</li>
 * <li>Consistency Management: Set the consistency level of individual schema elements</li>
 * </ul>
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanManagement extends TitanConfiguration, SchemaManager {

    /*
    ##################### RELATION TYPE INDEX ##########################
     */

    /**
     * Identical to {@link #buildEdgeIndex(com.thinkaurelius.titan.core.EdgeLabel, String, com.tinkerpop.gremlin.structure.Direction, com.tinkerpop.gremlin.structure.Order, com.thinkaurelius.titan.core.PropertyKey...)}
     * with default sort order {@link org.apache.tinkerpop.gremlin.process.traversal.Order#incr}.
     *
     * @param label
     * @param name
     * @param direction
     * @param sortKeys
     * @return the created {@link RelationTypeIndex}
     */
    public RelationTypeIndex buildEdgeIndex(EdgeLabel label, String name, Direction direction, PropertyKey... sortKeys);

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
    public RelationTypeIndex buildEdgeIndex(EdgeLabel label, String name, Direction direction, Order sortOrder, PropertyKey... sortKeys);

    /**
     * Identical to {@link #buildPropertyIndex(com.thinkaurelius.titan.core.PropertyKey, String, com.tinkerpop.gremlin.structure.Order, com.thinkaurelius.titan.core.PropertyKey...)}
     * with default sort order {@link com.tinkerpop.gremlin.structure.Order#incr}.
     *
     * @param key
     * @param name
     * @param sortKeys
     * @return the created {@link RelationTypeIndex}
     */
    public RelationTypeIndex buildPropertyIndex(PropertyKey key, String name, PropertyKey... sortKeys);

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
    public RelationTypeIndex buildPropertyIndex(PropertyKey key, String name, Order sortOrder, PropertyKey... sortKeys);

    /**
     * Whether a {@link RelationTypeIndex} with the given name has been defined for the provided {@link RelationType}
     *
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
     *
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
        public IndexBuilder addKey(PropertyKey key);

        /**
         * Adds the given key and associated parameters to the composite key of this index
         *
         * @param key
         * @param parameters
         * @return this IndexBuilder
         */
        public IndexBuilder addKey(PropertyKey key, Parameter... parameters);

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
         * Builds a composite index according to the specification
         *
         * @return the created composite {@link TitanGraphIndex}
         */
        public TitanGraphIndex buildCompositeIndex();

        /**
         * Builds a mixed index according to the specification against the backend index with the given name (i.e.
         * the name under which that index is configured in the graph configuration)
         *
         * @param backingIndex the name of the mixed index
         * @return the created mixed {@link TitanGraphIndex}
         */
        public TitanGraphIndex buildMixedIndex(String backingIndex);

    }

    public interface IndexJobFuture extends Future<ScanMetrics> {

        /**
         * Returns a set of potentially incomplete and still-changing metrics
         * for this job.  This is not guaranteed to be the same object as the
         * one returned by {@link #get()}, nor will the metrics visible through
         * the object returned by this method necessarily eventually converge
         * on the same values in the object returned by {@link #get()}, though
         * the implementation should attempt to provide both properties when
         * practical.
         * <p/>
         * The metrics visible through the object returned by this method may
         * also change their values between reads.  In other words, this is not
         * necessarily an immutable snapshot.
         * <p/>
         * If the index job has failed and the implementation is capable of
         * quickly detecting that, then the implementation should throw an
         * {@code ExecutionException}.  Returning metrics in case of failure is
         * acceptable, but throwing an exception is preferred.
         *
         * @return metrics for a potentially still-running job
         * @throws ExecutionException if the index job threw an exception
         */
        public ScanMetrics getIntermediateResult() throws ExecutionException;
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
     * and composite graph indexes allow changing of the consistency level.
     *
     * @param element
     * @param consistency
     */
    public void setConsistency(TitanSchemaElement element, ConsistencyModifier consistency);

    /**
     * Retrieves the time-to-live for the given {@link TitanSchemaType} as a {@link Duration}.
     * If no TTL has been defined, the returned Duration will be zero-length ("lives forever").
     *
     * @param type
     * @return
     */
    public Duration getTTL(TitanSchemaType type);

    /**
     * Sets the time-to-live for the given {@link TitanSchemaType}. The most granular time unit used for TTL values
     * is seconds. Any argument will be rounded to seconds if it is more granular than that.
     * The {@code ttl} must be nonnegative.  When {@code ttl} is zero, any existing TTL on {@code type} is removed
     * ("lives forever"). Positive {@code ttl} values are interpreted literally.
     *
     * @param type the affected type
     * @param ttl  time-to-live
     * @param unit time unit of the specified ttl
     */
    public void setTTL(TitanSchemaType type, Duration duration);

    /*
    ##################### SCHEMA UPDATE ##########################
     */

    /**
     * Changes the name of a {@link TitanSchemaElement} to the provided new name.
     * The new name must be valid and not already in use, otherwise an {@link IllegalArgumentException} is thrown.
     *
     * @param element
     * @param newName
     */
    public void changeName(TitanSchemaElement element, String newName);

    /**
     * Updates the provided index according to the given {@link SchemaAction}
     *
     * @param index
     * @param updateAction
     * @return a future that completes when the index action is done
     */
    public IndexJobFuture updateIndex(TitanIndex index, SchemaAction updateAction);

    /**
     * If an index update job was triggered through {@link #updateIndex(TitanIndex, SchemaAction)} with schema actions
     * {@link com.thinkaurelius.titan.core.schema.SchemaAction#REINDEX} or {@link com.thinkaurelius.titan.core.schema.SchemaAction#REMOVE_INDEX}
     * then this method can be used to track the status of this asynchronous process.
     *
     * @param index
     * @return A message that reflects the status of the index job
     */
    public IndexJobFuture getIndexJobStatus(TitanIndex index);

    /*
    ##################### CLUSTER MANAGEMENT ##########################
     */

    /**
     * Returns a set of unique instance ids for all Titan instances that are currently
     * part of this graph cluster.
     *
     * @return
     */
    public Set<String> getOpenInstances();

    /**
     * Forcefully removes a Titan instance from this graph cluster as identified by its name.
     * <p/>
     * This method should be used with great care and only in cases where a Titan instance
     * has been abnormally terminated (i.e. killed instead of properly shut-down). If this happens, the instance
     * will continue to be listed as an open instance which means that 1) a new instance with the same id cannot
     * be started and 2) schema updates will fail because the killed instance cannot acknowledge the schema update.
     * <p/>
     * <p/>
     * Throws an exception if the instance is not part of this cluster or if the instance has
     * been started after the start of this management transaction which is indicative of the instance
     * having been restarted successfully.
     *
     * @param instanceId
     */
    public void forceCloseInstance(String instanceId);

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
     * Returns an {@link Iterable} over all defined {@link VertexLabel}s.
     *
     * @return
     */
    public Iterable<VertexLabel> getVertexLabels();

    /**
     * Whether this management transaction is open or has been closed (i.e. committed or rolled-back)
     *
     * @return
     */
    public boolean isOpen();

    /**
     * Commits this management transaction and persists all schema changes. Closes this transaction.
     *
     * @see com.thinkaurelius.titan.core.TitanTransaction#commit()
     */
    public void commit();

    /**
     * Closes this management transaction and discards all changes.
     *
     * @see com.thinkaurelius.titan.core.TitanTransaction#rollback()
     */
    public void rollback();

}
