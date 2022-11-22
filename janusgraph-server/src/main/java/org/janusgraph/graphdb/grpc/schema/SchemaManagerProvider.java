// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.graphdb.grpc.schema;

import io.grpc.Status;
import io.grpc.StatusException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.grpc.schema.util.GrpcUtils;
import org.janusgraph.graphdb.grpc.types.EdgeLabel;
import org.janusgraph.graphdb.grpc.types.VertexCompositeGraphIndex;
import org.janusgraph.graphdb.grpc.types.VertexLabel;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SchemaManagerProvider {
    private final JanusGraphManagement management;

    public SchemaManagerProvider(JanusGraph graph) {
        this.management = graph.openManagement();
    }

    public VertexLabel getVertexLabelByName(String name) throws StatusException {
        org.janusgraph.core.VertexLabel vertexLabel = management.getVertexLabel(name);
        if (vertexLabel == null) {
            throw Status.NOT_FOUND.withDescription("No vertexLabel found with name: " + name).asException();
        }

        return GrpcUtils.createVertexLabelProto(vertexLabel);
    }

    public List<VertexLabel> getVertexLabels() {
        return StreamSupport
            .stream(management.getVertexLabels().spliterator(), false)
            .map(GrpcUtils::createVertexLabelProto).collect(Collectors.toList());
    }

    public EdgeLabel getEdgeLabelByName(String name) throws StatusException {
        org.janusgraph.core.EdgeLabel edgeLabel = management.getEdgeLabel(name);
        if (edgeLabel == null) {
            throw Status.NOT_FOUND.withDescription("No edgeLabel found with name: " + name).asException();
        }

        return GrpcUtils.createEdgeLabelProto(edgeLabel);
    }

    public List<EdgeLabel> getEdgeLabels() {
        return StreamSupport
            .stream(management.getRelationTypes(org.janusgraph.core.EdgeLabel.class).spliterator(), false)
            .map(GrpcUtils::createEdgeLabelProto).collect(Collectors.toList());
    }

    public VertexCompositeGraphIndex getVertexCompositeGraphIndexByName(String indexName) throws StatusException {
        JanusGraphIndex graphIndex = management.getGraphIndex(indexName);
        if (graphIndex == null) {
            throw Status.NOT_FOUND
                .withDescription("No composite graph index found with name: " + indexName).asException();
        }
        if (!graphIndex.isCompositeIndex()) {
            throw Status.FAILED_PRECONDITION
                .withDescription("Graph index isn't a composite index with name: " + indexName).asException();
        }
        if (!Vertex.class.isAssignableFrom(graphIndex.getIndexedElement())) {
            throw Status.FAILED_PRECONDITION
                .withDescription("Composite graph index isn't assignable to a vertex with name: " + indexName)
                .asException();
        }
        return GrpcUtils.createVertexCompositeGraphIndex(graphIndex);
    }

    public List<VertexCompositeGraphIndex> getVertexCompositeGraphIndices() {
        Iterable<JanusGraphIndex> indices = management.getGraphIndexes(Vertex.class);
        return StreamSupport
            .stream(indices.spliterator(), false)
            .filter(JanusGraphIndex::isCompositeIndex)
            .map(GrpcUtils::createVertexCompositeGraphIndex)
            .collect(Collectors.toList());
    }
}
