package org.janusgraph.core.schema;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface SchemaManager extends SchemaInspector {

    /**
     * Returns a {@link org.janusgraph.core.schema.PropertyKeyMaker} instance to define a new {@link com.thinkaurelius.titan.core.PropertyKey} with the given name.
     * By defining types explicitly (rather than implicitly through usage) one can control various
     * aspects of the key and associated consistency constraints.
     * <p/>
     * The key constructed with this maker will be created in the context of this transaction.
     *
     * @return a {@link org.janusgraph.core.schema.PropertyKeyMaker} linked to this transaction.
     * @see org.janusgraph.core.schema.PropertyKeyMaker
     * @see org.janusgraph.core.PropertyKey
     */
    public PropertyKeyMaker makePropertyKey(String name);

    /**
     * Returns a {@link org.janusgraph.core.schema.EdgeLabelMaker} instance to define a new {@link com.thinkaurelius.titan.core.EdgeLabel} with the given name.
     * By defining types explicitly (rather than implicitly through usage) one can control various
     * aspects of the label and associated consistency constraints.
     * <p/>
     * The label constructed with this maker will be created in the context of this transaction.
     *
     * @return a {@link org.janusgraph.core.schema.EdgeLabelMaker} linked to this transaction.
     * @see org.janusgraph.core.schema.EdgeLabelMaker
     * @see org.janusgraph.core.EdgeLabel
     */
    public EdgeLabelMaker makeEdgeLabel(String name);

    /**
     * Returns a {@link VertexLabelMaker} to define a new vertex label with the given name. Note, that the name must
     * be unique.
     *
     * @param name
     * @return
     */
    public VertexLabelMaker makeVertexLabel(String name);


}
