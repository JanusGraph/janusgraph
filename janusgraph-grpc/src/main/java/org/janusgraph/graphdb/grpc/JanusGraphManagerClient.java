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

package org.janusgraph.graphdb.grpc;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.janusgraph.graphdb.grpc.types.JanusGraphContext;

import java.util.List;

public class JanusGraphManagerClient {

    private final JanusGraphManagerServiceGrpc.JanusGraphManagerServiceBlockingStub blockingStub;

    public JanusGraphManagerClient(Channel channel) {
        blockingStub = JanusGraphManagerServiceGrpc.newBlockingStub(channel);
    }

    public JanusGraphManagerClient(String target) {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
            .usePlaintext()
            .build();
        blockingStub = JanusGraphManagerServiceGrpc.newBlockingStub(channel);
    }

    public JanusGraphContext getContextByGraphName(String name) {
        GetJanusGraphContextByGraphNameRequest request = GetJanusGraphContextByGraphNameRequest.newBuilder().setGraphName(name).build();
        GetJanusGraphContextByGraphNameResponse response = blockingStub.getJanusGraphContextByGraphName(request);
        return response.getContext();
    }

    public List<JanusGraphContext> getContexts() {
        GetJanusGraphContextsRequest request = GetJanusGraphContextsRequest.newBuilder().build();
        GetJanusGraphContextsResponse janusGraphContexts = blockingStub.getJanusGraphContexts(request);
        return janusGraphContexts.getContextsList();
    }

    public String getTinkerPopVersion() {
        GetVersionRequest request = GetVersionRequest.newBuilder().build();
        GetVersionResponse response = blockingStub.getVersion(request);
        return response.getTinkerpopVersion();
    }

    public String getJanusGraphVersion() {
        GetVersionRequest request = GetVersionRequest.newBuilder().build();
        GetVersionResponse response = blockingStub.getVersion(request);
        return response.getJanusgraphVersion();
    }
}
