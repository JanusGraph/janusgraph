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

import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface SchemaManager extends SchemaInspector {

    /**
     * Returns a {@link org.janusgraph.core.schema.PropertyKeyMaker} instance to define a new {@link org.janusgraph.core.PropertyKey} with the given name.
     * By defining types explicitly (rather than implicitly through usage) one can control various
     * aspects of the key and associated consistency constraints.
     * <p>
     * The key constructed with this maker will be created in the context of this transaction.
     *
     * @return a {@link org.janusgraph.core.schema.PropertyKeyMaker} linked to this transaction.
     * @see org.janusgraph.core.schema.PropertyKeyMaker
     * @see org.janusgraph.core.PropertyKey
     */
    PropertyKeyMaker makePropertyKey(String name);

    /**
     * Returns a {@link org.janusgraph.core.schema.EdgeLabelMaker} instance to define a new {@link org.janusgraph.core.EdgeLabel} with the given name.
     * By defining types explicitly (rather than implicitly through usage) one can control various
     * aspects of the label and associated consistency constraints.
     * <p>
     * The label constructed with this maker will be created in the context of this transaction.
     *
     * @return a {@link org.janusgraph.core.schema.EdgeLabelMaker} linked to this transaction.
     * @see org.janusgraph.core.schema.EdgeLabelMaker
     * @see org.janusgraph.core.EdgeLabel
     */
    EdgeLabelMaker makeEdgeLabel(String name);

    /**
     * Returns a {@link VertexLabelMaker} to define a new vertex label with the given name. Note, that the name must
     * be unique.
     *
     * @param name
     * @return
     */
    VertexLabelMaker makeVertexLabel(String name);

    /**
     * Add property constraints for a given vertex label.
     *
     * @param vertexLabel to which the constraints applies.
     * @param keys defines the properties which should be added to the {@link VertexLabel} as constraints.
     * @return a {@link VertexLabel} edited which contains the added constraints.
     */
    VertexLabel addProperties(VertexLabel vertexLabel, PropertyKey... keys);

    /**
     * Add property constraints for a given edge label.
     *
     * @param edgeLabel to which the constraints applies.
     * @param keys defines the properties which should be added to the {@link EdgeLabel} as constraints.
     * @return a {@link EdgeLabel} edited which contains the added constraints.
     */
    EdgeLabel addProperties(EdgeLabel edgeLabel, PropertyKey... keys);

    /**
     * Add a constraint on which vertices the given edge label can connect.
     *
     * @param edgeLabel to which the constraint applies.
     * @param outVLabel specifies the outgoing vertex for this connection.
     * @param inVLabel specifies the incoming vertex for this connection.
     * @return a {@link EdgeLabel} edited which contains the added constraint.
     */
    EdgeLabel addConnection(EdgeLabel edgeLabel, VertexLabel outVLabel, VertexLabel inVLabel);


}
