package com.thinkaurelius.titan.core;

/**
 * Used to define new {@link TitanLabel}s
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface LabelMaker extends TypeMaker {

    /**
     * Configures the label to allow at most one incoming edge of this label
     * for each vertex in the graph. For instance, the label "fatherOf" is
     * biologically a oneToMany edge label.
     * </p>
     * This consistency constraint is enforced by the database at runtime.
     * The consistency parameter specifies how this constraint is ensured.
     *
     * @param consistency
     * @return
     */
    public LabelMaker oneToMany(UniquenessConsistency consistency);

    /**
     * As {@link #oneToMany(com.thinkaurelius.titan.core.TypeMaker.UniquenessConsistency)} with
     * the default consistency {@link UniquenessConsistency#LOCK}
     *
     * @return
     */
    public LabelMaker oneToMany();


    /**
     * Configures the label to allow at most one outgoing edge of this label
     * for each vertex in the graph. For instance, the label "sonOf" is
     * biologically a manyToOne edge label.
     * </p>
     * This consistency constraint is enforced by the database at runtime.
     * The consistency parameter specifies how this constraint is ensured.
     *
     * @param consistency
     * @return
     */
    public LabelMaker manyToOne(UniquenessConsistency consistency);

    /**
     * As {@link #manyToOne(com.thinkaurelius.titan.core.TypeMaker.UniquenessConsistency)} with
     * the default consistency {@link UniquenessConsistency#LOCK}
     *
     * @return
     */

    public LabelMaker manyToOne();


    /**
     * Configures the label to allow at most one outgoing and one incoming edge
     * of this label for each vertex in the graph.
     * </p>
     * This consistency constraint is enforced by the database at runtime.
     * The consistency parameter specifies how this constraint is ensured.
     *
     * @param consistency
     * @return
     */
    public LabelMaker oneToOne(UniquenessConsistency consistency);

    /**
     * As {@link #oneToOne(com.thinkaurelius.titan.core.TypeMaker.UniquenessConsistency)} with
     * the default consistency {@link UniquenessConsistency#LOCK}
     *
     * @return
     */
    public LabelMaker oneToOne();

    /**
     * Configures the label to allow arbitrarily many in- and outgoing edges
     * of this label per vertex. This is the default configuration for a label.
     *
     * @return
     */
    public LabelMaker manyToMany();


    /**
     * Configures the label to be directed.
     * <p/>
     * By default, the label is directed.
     *
     * @return this LabelMaker
     * @see com.thinkaurelius.titan.core.TitanLabel#isDirected()
     */
    public LabelMaker directed();

    /**
     * Configures the label to be unidirected.
     * <p/>
     * By default, the type is directed.
     *
     * @return this LabelMaker
     * @see com.thinkaurelius.titan.core.TitanLabel#isUnidirected()
     */
    public LabelMaker unidirected();


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
    public LabelMaker sortKey(TitanType... types);

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
    public LabelMaker sortOrder(Order order);

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
    public LabelMaker signature(TitanType... types);

    /**
     * Defines the {@link TitanLabel} specified by this LabelMaker and returns the resulting TitanLabel
     *
     * @return
     */
    @Override
    public TitanLabel make();

}
