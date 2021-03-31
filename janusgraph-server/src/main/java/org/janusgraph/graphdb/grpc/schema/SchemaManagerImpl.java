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

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.janusgraph.graphdb.grpc.JanusGraphContextHandler;
import org.janusgraph.graphdb.grpc.types.EdgeLabel;
import org.janusgraph.graphdb.grpc.types.JanusGraphContext;
import org.janusgraph.graphdb.grpc.types.VertexLabel;

public class SchemaManagerImpl extends SchemaManagerServiceGrpc.SchemaManagerServiceImplBase {
    private final JanusGraphContextHandler contextHandler;

    public SchemaManagerImpl(JanusGraphContextHandler contextHandler) {
        this.contextHandler = contextHandler;
    }

    private JanusGraphContext getContext(GeneratedMessageV3 request) {
        if (request instanceof GetVertexLabelByNameRequest) {
            if (((GetVertexLabelByNameRequest) request).hasContext()) {
                return ((GetVertexLabelByNameRequest) request).getContext();
            }
        }
        if (request instanceof GetEdgeLabelByNameRequest) {
            if (((GetEdgeLabelByNameRequest) request).hasContext()) {
                return ((GetEdgeLabelByNameRequest) request).getContext();
            }
        }
        return null;
    }

    interface ErrorFunction {
        void run(Throwable var);
    }

    private SchemaManagerProvider getSchemaManagerProvider(GeneratedMessageV3 request, ErrorFunction errorFunction) {
        if (request == null) {
            errorFunction.run(Status.INTERNAL
                .withDescription("request is required").asRuntimeException());
            return null;
        }
        JanusGraphContext context = getContext(request);
        if (context == null) {
            errorFunction.run(Status.INVALID_ARGUMENT
                .withDescription("context is required").asException());
            return null;
        }
        SchemaManagerProvider provider = contextHandler.getSchemaManagerProviderByContext(context);
        if (provider == null) {
            errorFunction.run(Status.INVALID_ARGUMENT
                .withDescription("context is correct to find a schema manager provider").asException());
            return null;
        }
        return provider;
    }

    @Override
    public void getVertexLabelByName(
        GetVertexLabelByNameRequest request,
        StreamObserver<GetVertexLabelByNameResponse> responseObserver
    ) {
        SchemaManagerProvider provider = getSchemaManagerProvider(request, responseObserver::onError);
        if (provider == null) return;

        final String vertexLabelName = request.getName();
        if (vertexLabelName.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("name is required").asException());
            return;
        }
        VertexLabel vertexLabel = provider.getVertexLabelByName(vertexLabelName);
        if (vertexLabel == null) {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("No vertexLabel found with name: " + vertexLabelName).asException());
            return;
        }
        responseObserver.onNext(GetVertexLabelByNameResponse.newBuilder().setVertexLabel(vertexLabel).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getEdgeLabelByName(
        GetEdgeLabelByNameRequest request,
        StreamObserver<GetEdgeLabelByNameResponse> responseObserver
    ) {
        SchemaManagerProvider provider = getSchemaManagerProvider(request, responseObserver::onError);
        if (provider == null) return;

        final String edgeLabelName = request.getName();
        if (edgeLabelName.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("name is required").asException());
            return;
        }
        EdgeLabel edgeLabel = provider.getEdgeLabelByName(edgeLabelName);
        if (edgeLabel == null) {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("No edgeLabel found with name: " + edgeLabelName).asException());
            return;
        }
        responseObserver.onNext(GetEdgeLabelByNameResponse.newBuilder().setEdgeLabel(edgeLabel).build());
        responseObserver.onCompleted();
    }
}
