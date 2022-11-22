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
import io.grpc.stub.StreamObserver;
import org.janusgraph.graphdb.grpc.JanusGraphContextHandler;
import org.janusgraph.graphdb.grpc.types.EdgeLabel;
import org.janusgraph.graphdb.grpc.types.JanusGraphContext;
import org.janusgraph.graphdb.grpc.types.VertexCompositeGraphIndex;
import org.janusgraph.graphdb.grpc.types.VertexLabel;

import java.util.List;

public class SchemaManagerImpl extends SchemaManagerServiceGrpc.SchemaManagerServiceImplBase {
    private final JanusGraphContextHandler contextHandler;

    public SchemaManagerImpl(JanusGraphContextHandler contextHandler) {
        this.contextHandler = contextHandler;
    }

    interface ErrorFunction {
        void run(Throwable var);
    }

    private SchemaManagerProvider getSchemaManagerProvider(JanusGraphContext context, ErrorFunction errorFunction) {
        if (context == null) {
            errorFunction.run(Status.INVALID_ARGUMENT
                .withDescription("context is required").asException());
            return null;
        }
        SchemaManagerProvider provider = contextHandler.getSchemaManagerProviderByContext(context);
        if (provider == null) {
            errorFunction.run(Status.INVALID_ARGUMENT
                .withDescription("a schema manager provider was not found with the provided context").asException());
            return null;
        }
        return provider;
    }

    @Override
    public void getVertexLabelByName(
        GetVertexLabelByNameRequest request,
        StreamObserver<GetVertexLabelByNameResponse> responseObserver
    ) {
        SchemaManagerProvider provider = getSchemaManagerProvider(request.getContext(), responseObserver::onError);
        if (provider == null) return;

        final String vertexLabelName = request.getName();
        if (vertexLabelName.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("name is required").asException());
            return;
        }

        try {
            VertexLabel vertexLabel = provider.getVertexLabelByName(vertexLabelName);
            responseObserver.onNext(GetVertexLabelByNameResponse.newBuilder().setVertexLabel(vertexLabel).build());
            responseObserver.onCompleted();
        } catch (StatusException exception) {
            responseObserver.onError(exception);
        } catch (Exception exception) {
            responseObserver.onError(Status.INTERNAL.withCause(exception).asException());
        }
    }

    @Override
    public void getVertexLabels(
        GetVertexLabelsRequest request,
        StreamObserver<GetVertexLabelsResponse> responseObserver
    ) {
        SchemaManagerProvider provider = getSchemaManagerProvider(request.getContext(), responseObserver::onError);
        if (provider == null) return;

        try {
            List<VertexLabel> vertexLabels = provider.getVertexLabels();
            responseObserver.onNext(GetVertexLabelsResponse.newBuilder().addAllVertexLabels(vertexLabels).build());
            responseObserver.onCompleted();
        } catch (Exception exception) {
            responseObserver.onError(Status.INTERNAL.withCause(exception).asException());
        }
    }

    @Override
    public void getEdgeLabelByName(
        GetEdgeLabelByNameRequest request,
        StreamObserver<GetEdgeLabelByNameResponse> responseObserver
    ) {
        SchemaManagerProvider provider = getSchemaManagerProvider(request.getContext(), responseObserver::onError);
        if (provider == null) return;

        final String edgeLabelName = request.getName();
        if (edgeLabelName.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("name is required").asException());
            return;
        }

        try {
            EdgeLabel edgeLabel = provider.getEdgeLabelByName(edgeLabelName);
            responseObserver.onNext(GetEdgeLabelByNameResponse.newBuilder().setEdgeLabel(edgeLabel).build());
            responseObserver.onCompleted();
        } catch (StatusException exception) {
            responseObserver.onError(exception);
        } catch (Exception exception) {
            responseObserver.onError(Status.INTERNAL.withCause(exception).asException());
        }
    }

    @Override
    public void getEdgeLabels(
        GetEdgeLabelsRequest request,
        StreamObserver<GetEdgeLabelsResponse> responseObserver
    ) {
        SchemaManagerProvider provider = getSchemaManagerProvider(request.getContext(), responseObserver::onError);
        if (provider == null) return;

        try {
            List<EdgeLabel> edgeLabels = provider.getEdgeLabels();
            responseObserver.onNext(GetEdgeLabelsResponse.newBuilder().addAllEdgeLabels(edgeLabels).build());
            responseObserver.onCompleted();
        } catch (Exception exception) {
            responseObserver.onError(Status.INTERNAL.withCause(exception).asException());
        }
    }

    @Override
    public void getVertexCompositeGraphIndexByName(
        GetVertexCompositeGraphIndexByNameRequest request,
        StreamObserver<GetVertexCompositeGraphIndexByNameResponse> responseObserver
    ) {
        SchemaManagerProvider provider = getSchemaManagerProvider(request.getContext(), responseObserver::onError);
        if (provider == null) return;

        final String indexName = request.getName();
        if (indexName.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("name is required").asException());
            return;
        }
        try {
            VertexCompositeGraphIndex index = provider.getVertexCompositeGraphIndexByName(indexName);
            responseObserver.onNext(GetVertexCompositeGraphIndexByNameResponse.newBuilder().setIndex(index).build());
            responseObserver.onCompleted();
        } catch (StatusException exception) {
            responseObserver.onError(exception);
        } catch (Exception exception) {
            responseObserver.onError(Status.INTERNAL.withCause(exception).asException());
        }
    }

    @Override
    public void getVertexCompositeGraphIndices(
        GetVertexCompositeGraphIndicesRequest request,
        StreamObserver<GetVertexCompositeGraphIndicesResponse> responseObserver
    ) {
        SchemaManagerProvider provider = getSchemaManagerProvider(request.getContext(), responseObserver::onError);
        if (provider == null) return;

        try {
            List<VertexCompositeGraphIndex> indices = provider.getVertexCompositeGraphIndices();
            responseObserver.onNext(GetVertexCompositeGraphIndicesResponse.newBuilder().addAllIndices(indices).build());
            responseObserver.onCompleted();
        } catch (Exception exception) {
            responseObserver.onError(Status.INTERNAL.withCause(exception).asException());
        }
    }
}
