package com.thinkaurelius.titan.core;

/**
 * Used to define new {@link PropertyKey}s
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface PropertyKeyMaker extends RelationTypeMaker {


    /**
     * Configures the {@link Cardinality} of this property key.
     * @param cardinality
     * @return
     */
    public PropertyKeyMaker cardinality(Cardinality cardinality);

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
     * @see PropertyKey#getDataType()
     */
    public PropertyKeyMaker dataType(Class<?> clazz);

    /**
     * Configures the composite sort key for this label.
     * <p/>
     * Specifying the sort key of a type allows relations of this type to be efficiently retrieved in the order of
     * the sort key.
     * <br />
     * For instance, if the edge label <i>friend</i> has the sort key (<i>since</i>), which is a property key
     * with a timestamp data type, then one can efficiently retrieve all edges with label <i>friend</i> in a specified
     * time interval using {@link TitanVertexQuery#interval(PropertyKey, Comparable, Comparable)}.
     * <br />
     * In other words, relations are stored on disk in the order of the configured sort key. The sort key is empty
     * by default.
     * <br />
     * If multiple types are specified as sort key, then those are considered as a <i>composite</i> sort key, i.e. taken jointly
     * in the given order.
     * <p/>
     * {@link RelationType}s used in the sort key must be either property out-unique keys or out-unique unidirected edge lables.
     *
     * @param types TitanTypes composing the sort key. The order is relevant.
     * @return this LabelMaker
     */
    public PropertyKeyMaker sortKey(RelationType... types);

    /**
     * Defines in which order to sort the relations for efficient retrieval, i.e. either increasing ({@link Order#ASC}) or
     * decreasing ({@link Order#DESC}).
     *
     * Note, that only one sort order can be specified and that a sort key must be defined to use a sort order.
     *
     * @param order
     * @return
     * @see #sortKey(RelationType...)
     */
    public PropertyKeyMaker sortOrder(Order order);

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
     * {@link RelationType}s used in the signature must be either property out-unique keys or out-unique unidirected edge labels.
     * <br />
     * The signature should not contain any types already included in the sort key. The sort key provides the same
     * storage and retrieval efficiency.
     * <br />
     * The signature is empty by default.
     *
     * @param types TitanTypes composing the signature key. The order is irrelevant.
     * @return this LabelMaker
     */
    public PropertyKeyMaker signature(RelationType... types);


    /**
     * Defines the {@link PropertyKey} specified by this KeyMaker and returns the resulting TitanKey
     *
     * @return
     */
    @Override
    public PropertyKey make();
}
