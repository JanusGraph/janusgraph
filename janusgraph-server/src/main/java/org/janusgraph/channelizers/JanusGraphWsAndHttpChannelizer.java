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

package org.janusgraph.channelizers;

import org.janusgraph.graphdb.management.JanusGraphManager;

import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import com.google.common.base.Preconditions;

/**
 * This class initializes a TinkerPop WsAndHttpChannelizer and enables JanusGraph to automatically bind
 * dynamically created graph names and traversal names to the GremlinServer's Executor.
 */
public class JanusGraphWsAndHttpChannelizer extends WsAndHttpChannelizer implements JanusGraphChannelizer {

    private ServerGremlinExecutor serverGremlinExecutor;

    @Override
    public void init(final ServerGremlinExecutor serverGremlinExecutor) {
        this.serverGremlinExecutor = serverGremlinExecutor;
        super.init(serverGremlinExecutor);
        final GraphManager graphManager = serverGremlinExecutor.getGraphManager();
        Preconditions.checkArgument(graphManager instanceof JanusGraphManager, "Must use JanusGraphManager with a JanusGraphChannelizer.");
        ((JanusGraphManager) graphManager).configureGremlinExecutor(serverGremlinExecutor.getGremlinExecutor());
    }
}

