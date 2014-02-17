package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Element;

/**
 * Used to define new {@link TitanKey}s
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface KeyMaker extends TypeMaker {

    /**
     * Configures this key to allow multiple properties of this key per vertex.
     * For instance, the property key "phoneNumber" is typically a list()-enabled
     * key to be able to store multiple phone numbers per individual.
     * </p>
     * If a key is configured to be list-valued, the properties must be added
     * with {@link TitanVertex#addProperty(TitanKey, Object)}, retrieved
     * via {@link TitanVertex#getProperties(TitanKey)} and individually deleted.
     *
     * @return this KeyMaker
     */
    public KeyMaker list();

    /**
     * Configures this key to allow at most one property of this key per vertex.
     * In other words, the key uniquely maps onto a value for a particular vertex.
     * For instance, the key "birthdate" is unique for each person.
     * </p>
     * Use {@link TitanVertex#setProperty(TitanKey, Object)} to set or replace
     * this property.
     * </p>
     * This consistency constraint is enforced by the database at runtime.
     * The consistency parameter specifies how this constraint is ensured.
     *
     * @param consistency
     * @return
     */
    public KeyMaker single(UniquenessConsistency consistency);

    /**
     * As {@link #single(com.thinkaurelius.titan.core.TypeMaker.UniquenessConsistency)} with
     * the default consistency {@link UniquenessConsistency#LOCK}
     *
     * @return
     */
    public KeyMaker single();

    /**
     * Configures this key such that its values are uniquely associated with at most one
     * vertex across the entire graph. For instance, the key "socialSecurityNumber" is a unique
     * key since each SSN belongs to at most one person vertex.
     * </p>
     * This consistency constraint is enforced by the database at runtime.
     * The consistency parameter specifies how this constraint is ensured.
     *
     * @param consistency
     * @return
     */
    public KeyMaker unique(UniquenessConsistency consistency);

    /**
     * As {@link #unique(com.thinkaurelius.titan.core.TypeMaker.UniquenessConsistency)} with
     * the default consistency {@link UniquenessConsistency#LOCK}
     *
     * @return
     */
    public KeyMaker unique();


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
    public KeyMaker indexed(Class<? extends Element> clazz);

    /**
     * Configures instances of this type to be indexed for the specified Element type using the external index with the given name.
     * This index must be configured prior to startup. One can either index vertices or edges.
     * <p/>
     *
     * By default, the type is not indexed.
     *
     * <p/>
     *
     * Optionally, one can provide parameters that are passed on to the indexing backend to configure how exactly this key
     * should be indexed. Which paramters are accepted depends on the storage backend that is configured for the given indexName.
     *
     * @return this type maker
     * @see com.thinkaurelius.titan.core.TitanKey#hasIndex(String, Class)
     * @since 0.3.0
     */
    public KeyMaker indexed(String indexName, Class<? extends Element> clazz, Parameter... indexParameters);

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
    public KeyMaker dataType(Class<?> clazz);

    /**
     * Configures the composite sort key for this label.
     * <p/>
     * Specifying the sort key of a type allows relations of this type to be efficiently retrieved in the order of
     * the sort key.
     * <br />
     * For instance, if the edge label <i>friend</i> has the sort key (<i>since</i>), which is a property key
     * with a timestamp data type, then one can efficiently retrieve all edges with label <i>friend</i> in a specified
     * time interval using {@link TitanVertexQuery#interval(TitanKey, Comparable, Comparable)}.
     * <br />
     * In other words, relations are stored on disk in the order of the configured sort key. The sort key is empty
     * by default.
     * <br />
     * If multiple types are specified as sort key, then those are considered as a <i>composite</i> sort key, i.e. taken jointly
     * in the given order.
     * <p/>
     * {@link TitanType}s used in the sort key must be either property out-unique keys or out-unique unidirected edge lables.
     *
     * @param types TitanTypes composing the sort key. The order is relevant.
     * @return this LabelMaker
     */
    public KeyMaker sortKey(TitanType... types);

    /**
     * Defines in which order to sort the relations for efficient retrieval, i.e. either increasing ({@link Order#ASC}) or
     * decreasing ({@link Order#DESC}).
     *
     * Note, that only one sort order can be specified and that a sort key must be defined to use a sort order.
     *
     * @param order
     * @return
     * @see #sortKey(TitanType...)
     */
    public KeyMaker sortOrder(Order order);

    /**
     * Configures the signature of this label.
     * <p/>
     * Specifying the signature of a type tells the graph database to <i>expect</i> that relations of this type
     * always have or are likely to have an incident property or unidirected edge of the type included in the
     * signature. This allows the graph database to store such edges more compactly and retrieve them more quickly.
     * <br />
     * For instance, if all edges with label <i>friend</i> have a property with key <i>createdOn</i>, then specifying
     * (<i>createdOn</i>) as the signature for type <i>friend</i> allows friend edges to be stored more efficiently.
     * <br />
     * {@link TitanType}s used in the signature must be either property out-unique keys or out-unique unidirected edge labels.
     * <br />
     * The signature should not contain any types already included in the sort key. The sort key provides the same
     * storage and retrieval efficiency.
     * <br />
     * The signature is empty by default.
     *
     * @param types TitanTypes composing the signature key. The order is irrelevant.
     * @return this LabelMaker
     */
    public KeyMaker signature(TitanType... types);


    /**
     * Defines the {@link TitanKey} specified by this KeyMaker and returns the resulting TitanKey
     *
     * @return
     */
    @Override
    public TitanKey make();
}
