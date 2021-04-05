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

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.janusgraph.graphdb.grpc.types.EdgeLabel;
import org.janusgraph.graphdb.grpc.types.JanusGraphContext;
import org.janusgraph.graphdb.grpc.types.VertexLabel;

import java.util.List;

public class SchemaManagerClient {

    private final SchemaManagerServiceGrpc.SchemaManagerServiceBlockingStub blockingStub;
    private final JanusGraphContext context;

    public SchemaManagerClient(JanusGraphContext context, Channel channel) {
        this.context = context;
        blockingStub = SchemaManagerServiceGrpc.newBlockingStub(channel);
    }

    public SchemaManagerClient(JanusGraphContext context, String target) {
        this.context = context;
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
            .usePlaintext()
            .build();
        blockingStub = SchemaManagerServiceGrpc.newBlockingStub(channel);
    }

    /**
     * @param name Get a VertexLabel by name
     * @return {@link VertexLabel}
     */
    public VertexLabel getVertexLabelByName(String name) {
        GetVertexLabelByNameRequest request = GetVertexLabelByNameRequest.newBuilder().setContext(context).setName(name).build();
        GetVertexLabelByNameResponse response = blockingStub.getVertexLabelByName(request);
        return response.getVertexLabel();
    }

    /**
     * @return Returns a list of {@link VertexLabel}
     */
    public List<VertexLabel> getVertexLabels() {
        GetVertexLabelsRequest request = GetVertexLabelsRequest.newBuilder().setContext(context).build();
        GetVertexLabelsResponse response = blockingStub.getVertexLabels(request);
        return response.getVertexLabelsList();
    }

    /**
     * @param name Get an EdgeLabel by name
     * @return {@link EdgeLabel}
     */
    public EdgeLabel getEdgeLabelByName(String name) {
        GetEdgeLabelByNameRequest request = GetEdgeLabelByNameRequest.newBuilder().setContext(context).setName(name).build();
        GetEdgeLabelByNameResponse response = blockingStub.getEdgeLabelByName(request);
        return response.getEdgeLabel();
    }

    /**
     * @return Returns a list of {@link EdgeLabel}
     */
    public List<EdgeLabel> getEdgeLabels() {
        GetEdgeLabelsRequest request = GetEdgeLabelsRequest.newBuilder().setContext(context).build();
        GetEdgeLabelsResponse response = blockingStub.getEdgeLabels(request);
        return response.getEdgeLabelsList();
    }
}
