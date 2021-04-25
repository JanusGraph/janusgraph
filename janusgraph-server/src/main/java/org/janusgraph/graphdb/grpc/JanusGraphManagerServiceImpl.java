// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.graphdb.grpc;

import com.jcabi.manifests.Manifests;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.janusgraph.graphdb.grpc.types.JanusGraphContext;
import org.janusgraph.graphdb.server.JanusGraphServer;

public class JanusGraphManagerServiceImpl extends JanusGraphManagerServiceGrpc.JanusGraphManagerServiceImplBase {
    private final JanusGraphContextHandler contextHandler;

    public JanusGraphManagerServiceImpl(JanusGraphContextHandler contextHandler) {
        this.contextHandler = contextHandler;
    }

    @Override
    public void getJanusGraphContexts(GetJanusGraphContextsRequest request, StreamObserver<GetJanusGraphContextsResponse> responseObserver) {
        GetJanusGraphContextsResponse.Builder response = GetJanusGraphContextsResponse.newBuilder();

        for (JanusGraphContext context : contextHandler.getContexts()) {
            response.addContexts(context);
        }
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getJanusGraphContextByGraphName(GetJanusGraphContextByGraphNameRequest request,
                                                StreamObserver<GetJanusGraphContextByGraphNameResponse> responseObserver) {
        final String graphName = request.getGraphName();
        if (graphName.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("graphName is required").asException());
            return;
        }
        JanusGraphContext context = contextHandler.getContextByGraphName(graphName);
        if (context == null) {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("JanusGraphContext wasn't found for graphName").asException());
        }
        responseObserver.onNext(GetJanusGraphContextByGraphNameResponse.newBuilder()
            .setContext(context)
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getVersion(GetVersionRequest request, StreamObserver<GetVersionResponse> responseObserver) {
        String tinkerPopVersion = "debug-tp";
        String janusGraphVersion = "debug-jg";
        if (Manifests.exists(JanusGraphServer.MANIFEST_TINKERPOP_VERSION_ATTRIBUTE)){
            tinkerPopVersion = Manifests.read(JanusGraphServer.MANIFEST_TINKERPOP_VERSION_ATTRIBUTE);
        }
        if (Manifests.exists(JanusGraphServer.MANIFEST_JANUSGRAPH_VERSION_ATTRIBUTE)){
            janusGraphVersion = Manifests.read(JanusGraphServer.MANIFEST_JANUSGRAPH_VERSION_ATTRIBUTE);
        }
        responseObserver.onNext(GetVersionResponse.newBuilder()
            .setTinkerpopVersion(tinkerPopVersion)
            .setJanusgraphVersion(janusGraphVersion)
            .build());
        responseObserver.onCompleted();
    }
}
