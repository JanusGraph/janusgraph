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

import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.graphdb.grpc.schema.SchemaManagerProvider;
import org.janusgraph.graphdb.grpc.types.JanusGraphContext;

import java.util.ArrayList;

public class JanusGraphContextHandler {
    private final GraphManager graphManager;

    public JanusGraphContextHandler(GraphManager graphManager) {
        this.graphManager = graphManager;
    }

    public Iterable<JanusGraphContext> getContexts() {
        ArrayList<JanusGraphContext> contexts = new ArrayList<>();
        for (String graphName : graphManager.getGraphNames()) {
            Graph graph = graphManager.getGraph(graphName);
            if (!(graph instanceof JanusGraph)) {
                continue;
            }
            contexts.add(JanusGraphContext.newBuilder().setGraphName(graphName).build());
        }
        return contexts;
    }

    public JanusGraphContext getContextByGraphName(String graphName) {
        Graph graph = graphManager.getGraph(graphName);
        if (!(graph instanceof JanusGraph)) {
            return null;
        }
        return JanusGraphContext.newBuilder().setGraphName(graphName).build();
    }

    public SchemaManagerProvider getSchemaManagerProviderByContext(JanusGraphContext context) {
        Graph graph = graphManager.getGraph(context.getGraphName());
        if (!(graph instanceof JanusGraph)) {
            return null;
        }
        return new SchemaManagerProvider((JanusGraph) graph);
    }
}
