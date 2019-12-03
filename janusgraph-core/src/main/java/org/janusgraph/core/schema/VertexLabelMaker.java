// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.core.schema;

import org.janusgraph.core.VertexLabel;

/**
 * A builder to create new {@link VertexLabel}s.
 *
 * A vertex label is defined by its name and additional properties such as:
 * <ul>
 *     <li>Partition: Whether the vertices of this label should be partitioned. A partitioned vertex is split across the partitions
 *     in a graph such that each partition contains on "sub-vertex". This allows JanusGraph to effectively manage
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
    String getName();

    /**
     * Enables partitioning for this vertex label. If a vertex label is partitioned, all of its
     * vertices are partitioned across the partitions of the graph.
     *
     * @return this VertexLabelMaker
     */
    VertexLabelMaker partition();

    /**
     * Makes this vertex label static, which means that vertices of this label cannot be modified outside of the transaction
     * in which they were created.
     *
     * @return this VertexLabelMaker
     */
    VertexLabelMaker setStatic();

    /**
     * Creates a {@link VertexLabel} according to the specifications of this builder.
     *
     * @return the created vertex label
     */
    VertexLabel make();
}
