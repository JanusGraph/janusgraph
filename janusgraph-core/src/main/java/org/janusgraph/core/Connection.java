// Copyright 2018 JanusGraph Authors
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

package org.janusgraph.core;

import org.janusgraph.graphdb.types.TypeDefinitionDescription;
import org.janusgraph.graphdb.types.system.BaseKey;

/**
 * Connection contains schema constraints from outgoing vertex to incoming vertex through an edge.
 *
 * @author Jan Jansen (jan.jansen@gdata.de)
 */
public class Connection {
    private final VertexLabel incomingVertexLabel;
    private final VertexLabel outgoingVertexLabel;
    private final String edgeLabel;
    private final JanusGraphEdge connectionEdge;

    public Connection(JanusGraphEdge connectionEdge) {
        this.outgoingVertexLabel = (VertexLabel) connectionEdge.outVertex();
        this.incomingVertexLabel = (VertexLabel) connectionEdge.inVertex();
        TypeDefinitionDescription value = connectionEdge.valueOrNull(BaseKey.SchemaDefinitionDesc);
        this.edgeLabel = (String) value.getModifier();
        this.connectionEdge = connectionEdge;
    }


    public Connection(String edgeLabel, VertexLabel outgoingVertexLabel, VertexLabel incomingVertexLabel) {
        this.outgoingVertexLabel = outgoingVertexLabel;
        this.incomingVertexLabel = incomingVertexLabel;
        this.edgeLabel = edgeLabel;
        this.connectionEdge = null;
    }

    /**
     *
     * @return a label from an {@link EdgeLabel}.
     */
    public String getEdgeLabel() {
        return edgeLabel;
    }

    /**
     *
     * @return a outgoing {@link VertexLabel}.
     */
    public VertexLabel getOutgoingVertexLabel() {
        return outgoingVertexLabel;
    }

    /**
     *
     * @return a incoming {@link VertexLabel}.
     */
    public VertexLabel getIncomingVertexLabel() {
        return incomingVertexLabel;
    }


    /**
     *
     * @return a internal connection edge is used for update.
     */
    public JanusGraphEdge getConnectionEdge() {
        return connectionEdge;
    }
}
