package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.VertexLabel;

/**
 * A builder to create new {@link VertexLabel}s.
 *
 * A vertex label is defined by its name and additional properties such as:
 * <ul>
 *     <li>Partition: Whether the vertices of this label should be partitioned. A partitioned vertex is split across the partitions
 *     in a graph such that each partition contains on "sub-vertex". This allows Titan to effectively manage
 *     vertices with very large degrees but is inefficient for vertices with small degree</li>
 * </ul>
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexLabelMaker {

    /**
     * Returns the name of the to-be-build vertex label
     * @return the label name
     */
    public String getName();

    /**
     * Enables partitioning for this vertex label. If a vertex label is partitioned, all of its
     * vertices are partitioned across the partitions of the graph.
     *
     * @return this VertexLabelMaker
     */
    public VertexLabelMaker partition();

    /**
     * Makes this vertex label static, which means that vertices of this label cannot be modified outside of the transaction
     * in which they were created.
     *
     * @return this VertexLabelMaker
     */
    public VertexLabelMaker setStatic();

    /**
     * Creates a {@link VertexLabel} according to the specifications of this builder.
     *
     * @return the created vertex label
     */
    public VertexLabel make();


}
