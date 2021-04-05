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

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.grpc.schema.util.GrpcUtils;
import org.janusgraph.graphdb.grpc.types.EdgeLabel;
import org.janusgraph.graphdb.grpc.types.VertexLabel;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SchemaManagerProvider {
    private final JanusGraphManagement management;

    public SchemaManagerProvider(JanusGraph graph) {
        this.management = graph.openManagement();
    }

    public VertexLabel getVertexLabelByName(String name) {
        org.janusgraph.core.VertexLabel vertexLabel = management.getVertexLabel(name);
        if (vertexLabel == null) {
            return null;
        }

        return GrpcUtils.createVertexLabelProto(vertexLabel);
    }

    public List<VertexLabel> getVertexLabels() {
        return StreamSupport
            .stream(management.getVertexLabels().spliterator(), false)
            .map(GrpcUtils::createVertexLabelProto).collect(Collectors.toList());
    }

    public EdgeLabel getEdgeLabelByName(String name) {
        org.janusgraph.core.EdgeLabel edgeLabel = management.getEdgeLabel(name);
        if (edgeLabel == null) {
            return null;
        }

        return GrpcUtils.createEdgeLabelProto(edgeLabel);
    }

    public List<EdgeLabel> getEdgeLabels() {
        return StreamSupport
            .stream(management.getRelationTypes(org.janusgraph.core.EdgeLabel.class).spliterator(), false)
            .map(GrpcUtils::createEdgeLabelProto).collect(Collectors.toList());
    }
}
