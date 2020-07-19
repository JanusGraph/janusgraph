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

package org.janusgraph.graphdb.server;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang.NullArgumentException;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraph;

public class JanusGraphManagerImpl extends JanusGraphManagerGrpc.JanusGraphManagerImplBase {
    private final GraphManager graphManager;

    public JanusGraphManagerImpl(GraphManager graphManager) {
        this.graphManager = graphManager;
    }

    @Override
    public void getJanusGraphContexts(GetJanusGraphContextsRequest request, StreamObserver<JanusGraphContext> responseObserver) {
        if (request == null) {
            responseObserver.onError(Status.INTERNAL.withCause(new NullArgumentException("request should be set")).asRuntimeException());
            return;
        }
        for (String graphName : graphManager.getGraphNames()) {
            Graph graph = graphManager.getGraph(graphName);
            if (!(graph instanceof JanusGraph)) {
                continue;
            }
            responseObserver.onNext(JanusGraphContext.newBuilder().setGraphName(graphName).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void getJanusGraphContextByGraphName(GetJanusGraphContextByGraphNameRequest request,
                                                StreamObserver<JanusGraphContext> responseObserver) {
        if (request == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withCause(new NullArgumentException("request should be set")).asRuntimeException());
            return;
        }
        final String graphName = request.getGraphName();
        if (graphName.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withCause(new NullArgumentException("graphName should be set")).asRuntimeException());
            return;
        }
        Graph graph = graphManager.getGraph(graphName);
        if (!(graph instanceof JanusGraph)) {
            responseObserver.onError(Status.INTERNAL
                .withCause(new IllegalStateException("graphName should access a JanusGraph instance")).asException());
        }
        responseObserver.onNext(JanusGraphContext.newBuilder().setGraphName(graphName).build());
        responseObserver.onCompleted();
    }
}
