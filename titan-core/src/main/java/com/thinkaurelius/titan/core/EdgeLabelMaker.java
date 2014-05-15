package com.thinkaurelius.titan.core;

/**
 * Used to define new {@link EdgeLabel}s
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface EdgeLabelMaker extends RelationTypeMaker {

    /**
     * Sets the multiplicity of this label. The default multiplicity is {@link Multiplicity#MULTI}.
     * @return
     */
    public EdgeLabelMaker multiplicity(Multiplicity multiplicity);

    /**
     * Configures the label to be directed.
     * <p/>
     * By default, the label is directed.
     *
     * @return this LabelMaker
     * @see EdgeLabel#isDirected()
     */
    public EdgeLabelMaker directed();

    /**
     * Configures the label to be unidirected.
     * <p/>
     * By default, the type is directed.
     *
     * @return this LabelMaker
     * @see EdgeLabel#isUnidirected()
     */
    public EdgeLabelMaker unidirected();


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
    public EdgeLabelMaker sortKey(RelationType... types);

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
    public EdgeLabelMaker sortOrder(Order order);

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
    public EdgeLabelMaker signature(RelationType... types);

    /**
     * Defines the {@link EdgeLabel} specified by this LabelMaker and returns the resulting TitanLabel
     *
     * @return
     */
    @Override
    public EdgeLabel make();

}
