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
import io.grpc.stub.StreamObserver;
import org.janusgraph.graphdb.grpc.JanusGraphContextHandler;
import org.janusgraph.graphdb.grpc.types.VertexLabel;

public class SchemaManagerImpl extends SchemaManagerServiceGrpc.SchemaManagerServiceImplBase {
    private final JanusGraphContextHandler contextHandler;

    public SchemaManagerImpl(JanusGraphContextHandler contextHandler) {
        this.contextHandler = contextHandler;
    }

    @Override
    public void getVertexLabelByName(
        GetVertexLabelByNameRequest request,
        StreamObserver<GetVertexLabelByNameResponse> responseObserver
    ) {
        if (request == null) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("request is required").asRuntimeException());
            return;
        }
        if (!request.hasContext()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("context is required").asException());
            return;
        }
        final String vertexLabelName = request.getName();
        if (vertexLabelName.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("name is required").asException());
            return;
        }
        SchemaManagerProvider provider = contextHandler.getSchemaManagerProviderByContext(request.getContext());
        VertexLabel vertexLabel = provider.getVertexLabelByName(vertexLabelName);
        if (vertexLabel == null) {
            responseObserver.onError(Status.NOT_FOUND.asException());
            return;
        }
        responseObserver.onNext(GetVertexLabelByNameResponse.newBuilder().setVertexLabel(vertexLabel).build());
        responseObserver.onCompleted();
    }
}
