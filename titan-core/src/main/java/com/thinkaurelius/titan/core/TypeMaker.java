package com.thinkaurelius.titan.core;

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

    /**
     * Sets the name of the type
     *
     * @param name name of the type
     * @return this type maker
     * @see com.thinkaurelius.titan.core.TitanType#getName()
     */
    public TypeMaker name(String name);

    /**
     * Configures the type to be functional.
     * <p/>
     * By default, the type is non-functional.
     *
     * @return this type maker
     * @see com.thinkaurelius.titan.core.TitanType#isFunctional()
     */
    public TypeMaker functional();

    /**
     * Configures the type to be functional and whether the database
     * should acquire a lock when creating or updating relation instances of this type.
     * <p/>
     * If locking is set to true, which is the default when invoking {@link #functional()}, then the database
     * will acquire a lock an updated or created relation to ensure no data is blindly overwritten. If locking is
     * set to false, then no lock is acquired. Acquiring locks ensures data consistency but comes at the expense
     * of having to acquire a lock and failing transactions when there is lock contention or the lock could not be
     * acquired for other reasons.
     *
     * @return this type maker
     */
    public TypeMaker functional(boolean locking);

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
     * Configures the type to be undirected. This only applies to edge labels.
     * <p/>
     * By default, the type is directed.
     *
     * @return this type maker
     * @see com.thinkaurelius.titan.core.TitanLabel#isUndirected()
     */
    public TypeMaker undirected();

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
     * Configures the type to be simple, which means that relation instances of the type do not support incident properties.
     * <p/>
     * By default, the type is not simple and allows incident properties.
     *
     * @return this type maker
     * @see com.thinkaurelius.titan.core.TitanType#isSimple()
     */
    public TypeMaker simple();


    /**
     * Assigns the type to the specified {@link TypeGroup}.
     * <p/>
     * By default, the type is not assigned to {@link TypeGroup#DEFAULT_GROUP}.
     *
     * @param group group to assign type to.
     * @return this type maker
     * @see TypeGroup
     */
    public TypeMaker group(TypeGroup group);

    /**
     * Configures the composite primary key for this type. This only applies to edge labels.
     * <p/>
     * Specifying the primary key of a type allows edges of this type to be efficiently retrieved in the order of
     * the key.
     * <br />
     * For instance, if the edge label <i>friend</i> has the primary key (<i>since</i>), which is a property key
     * with a timestamp data type, then one can efficiently retrieve all edges with label <i>friend</i> in a specified
     * time interval using {@link TitanQuery#interval(TitanKey, Comparable, Comparable)}.
     * <br />
     * In other words, relations are stored on disk in the order of the configured primary key. The primary key is empty
     * by default.
     * <p/>
     * {@link TitanType}s used in the primary key must be either property keys or unidirected edge lables.
     * Also, they must be simple ({@link com.thinkaurelius.titan.core.TitanType#isSimple()}) and
     * functional ({@link com.thinkaurelius.titan.core.TitanType#isFunctional()}).
     *
     * @param types TitanTypes composing the primary key. The order is relevant.
     * @return this type maker
     */
    public TypeMaker primaryKey(TitanType... types);

    /**
     * Configures the signature of this type. This only applies to edge labels.
     * <p/>
     * Specifying the signature of a type tells the graph database to <i>expect</i> that edges of this type
     * always have or are likely to have an incident property or unidirected edge of the type included in the
     * signature. This allows the graph database to store such edges more compactly and retrieve them more quickly.
     * <br />
     * For instance, if all edges with label <i>friend</i> have a property with key <i>createdOn</i>, then specifying
     * (<i>createdOn</i>) as the signature for type <i>friend</i> allows friend edges to be stored more efficiently.
     * <br />
     * {@link TitanType}s used in the signature must be either property keys or unidirected edge lables.
     * Also, they must be simple ({@link com.thinkaurelius.titan.core.TitanType#isSimple()}) and
     * functional ({@link com.thinkaurelius.titan.core.TitanType#isFunctional()}).
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
     * Configures the type to be unqiue, which means that each value for a property of this type is uniquely associated
     * with a vertex. This only applies to property keys.
     * <p/>
     * By default, the type is not unique.
     *
     * @return this type maker
     * @see TitanKey#isUnique()
     */
    public TypeMaker unique();

    /**
     * Configures instances of this type to be indexed. This only applies to property keys.
     * <p/>
     * By default, the type is not indexed.
     *
     * @return this type maker
     * @see com.thinkaurelius.titan.core.TitanKey#hasIndex()
     */
    public TypeMaker indexed();

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
