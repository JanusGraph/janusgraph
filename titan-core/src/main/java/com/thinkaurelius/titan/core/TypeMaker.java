package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Element;

/**
 * TypeMaker is a factory for {@link TitanType}s. TitanTypes can be configured to provide data verification,
 * better storage efficiency, and higher performance. The TitanType defines the schema for all {@link TitanRelation}s
 * of that type.
 * <br />
 * All user defined types are configured using a TypeMaker instance returned by {@link com.thinkaurelius.titan.core.TitanTransaction#makeType()}.
 * Hence, types are defined within the context of a transaction like every other object in a TitanGraph. The TypeMaker
 * is used to create both: property keys and edge labels using either {@link #makePropertyKey()} or {@link #makeEdgeLabel()},
 * respectively. Some of the methods in TypeMaker are only applicable to one or the other.
 * <br />
 * Most configuration options provided by the methods of this class are optional and default values are assumed as
 * documented for the particular methods. However, one must call {@link #name(String)} to define the unqiue name of the type.
 * When defining property keys, one must also configure the data type using {@link #dataType(Class)}.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see TitanType
 * @see <a href="https://github.com/thinkaurelius/titan/wiki/Type-Definition-Overview">Titan Type Wiki</a>
 */
public interface TypeMaker {

    public enum UniquenessConsistency {

        /**
         * Does not acquire a lock and hence concurrent transactions may overwrite existing
         * uniqueness relations.
         */
        NO_LOCK,
        /**
         * Acquires a lock to ensure uniqueness consistency.
         */
        LOCK
    }


    /**
     * Sets the name of the type
     *
     * @param name name of the type
     * @return this type maker
     * @see com.thinkaurelius.titan.core.TitanType#getName()
     */
    public TypeMaker name(String name);

    /**
     * Configures the type to be unique in the given direction with the provided uniqueness consistency.
     * A type is unique in a given direction if there can be at most one relation (edge or property) in that direction for
     * a particular element. For instance, an out-unique edge label ensures that there can be at most one outgoing edge
     * of said label for any vertex. For property keys, in-uniqueness has the special meaning that there is at most one
     * vertex associated with the property value. Hence, it allows to ensure that values are uniquely assigned to vertices
     * in the database.
     *
     * <p />
     * The consistency parameter specifies the consistency guarantees given when ensuring uniqueness. Without acquiring locks,
     * unique relations may be overwritten by competing transactions. Acquring locks prohibits that at and additional "locking cost".
     *
     * @return this type maker
     */
    public TypeMaker unique(Direction direction, UniquenessConsistency consistency);

    /**
     * Configures the type to be unique in the given direction with the default uniqueness consistency {@link UniquenessConsistency#LOCK}.
     *
     * @param direction
     * @return
     * @see #unique(com.tinkerpop.blueprints.Direction, com.thinkaurelius.titan.core.TypeMaker.UniquenessConsistency)
     */
    public TypeMaker unique(Direction direction);

    /**
     * Configures the type to be directed. This only applies to edge labels.
     * <p/>
     * By default, the type is directed.
     *
     * @return this type maker
     * @see com.thinkaurelius.titan.core.TitanLabel#isDirected()
     */
    public TypeMaker directed();

    /**
     * Configures the type to be unidirected. This only applies to edge labels.
     * <p/>
     * By default, the type is directed.
     *
     * @return this type maker
     * @see com.thinkaurelius.titan.core.TitanLabel#isUnidirected()
     */
    public TypeMaker unidirected();

    /**
     * Configures the composite primary key for this type.
     * <p/>
     * Specifying the primary key of a type allows relations of this type to be efficiently retrieved in the order of
     * the primary key.
     * <br />
     * For instance, if the edge label <i>friend</i> has the primary key (<i>since</i>), which is a property key
     * with a timestamp data type, then one can efficiently retrieve all edges with label <i>friend</i> in a specified
     * time interval using {@link TitanVertexQuery#interval(TitanKey, Comparable, Comparable)}.
     * <br />
     * In other words, relations are stored on disk in the order of the configured primary key. The primary key is empty
     * by default.
     * <br />
     * If multiple types are specified as primary key, then those are considered as a <i>composite</i> primary key, i.e. taken jointly
     * in the given order.
     * <p/>
     * {@link TitanType}s used in the primary key must be either property out-unique keys or out-unique unidirected edge lables.
     *
     * @param types TitanTypes composing the primary key. The order is relevant.
     * @return this type maker
     */
    public TypeMaker primaryKey(TitanType... types);

    /**
     * Configures the signature of this type.
     * <p/>
     * Specifying the signature of a type tells the graph database to <i>expect</i> that relations of this type
     * always have or are likely to have an incident property or unidirected edge of the type included in the
     * signature. This allows the graph database to store such edges more compactly and retrieve them more quickly.
     * <br />
     * For instance, if all edges with label <i>friend</i> have a property with key <i>createdOn</i>, then specifying
     * (<i>createdOn</i>) as the signature for type <i>friend</i> allows friend edges to be stored more efficiently.
     * <br />
     * {@link TitanType}s used in the primary key must be either property out-unique keys or out-unique unidirected edge lables.
     * <br />
     * The signature should not contain any types already included in the primary key. The primary key provides the same
     * storage and retrieval efficiency.
     * <br />
     * The signature is empty by default.
     *
     * @param types TitanTypes composing the primary key. The order is irrelevant.
     * @return this type maker
     */
    public TypeMaker signature(TitanType... types);

    /**
     * Configures instances of this type to be indexed for the specified Element type using the <i>standard</i> Titan index.
     * One can either index vertices or edges.
     * <p/>
     * This only applies to property keys.
     * By default, the type is not indexed.
     *
     * @return this type maker
     * @see com.thinkaurelius.titan.core.TitanKey#hasIndex(String, Class)
     * @since 0.3.0
     */
    public TypeMaker indexed(Class<? extends Element> clazz);

    /**
     * Configures instances of this type to be indexed for the specified Element type using the external index with the given name.
     * This index must be configured prior to startup. One can either index vertices or edges.
     * <p/>
     * This only applies to property keys.
     * By default, the type is not indexed.
     *
     * @return this type maker
     * @see com.thinkaurelius.titan.core.TitanKey#hasIndex(String, Class)
     * @since 0.3.0
     */
    public TypeMaker indexed(String indexName, Class<? extends Element> clazz);

    /**
     * Configures the data type for this type.  This only applies to property keys.
     * <p/>
     * Property instances for this key will only accept attribute values that are instances of this class.
     * Every property key must have its data type configured. Setting the data type to Object.class allows
     * any type of attribute but comes at the expense of longer serialization because class information
     * is stored with the attribute value.
     *
     * @param clazz Data type to be configured.
     * @return this type maker
     * @see TitanKey#getDataType()
     */
    public TypeMaker dataType(Class<?> clazz);

    /**
     * Creates an edge label according to the configuration of this TypeMaker.
     *
     * @return New edge label
     * @throws IllegalArgumentException if the name is already in use or if other configurations are invalid.
     */
    public TitanLabel makeEdgeLabel();

    /**
     * Creates a property key according to the configuration of this TypeMaker.
     *
     * @return New property key
     * @throws IllegalArgumentException if the name is already in use or if other configurations are invalid.
     */
    public TitanKey makePropertyKey();

}
