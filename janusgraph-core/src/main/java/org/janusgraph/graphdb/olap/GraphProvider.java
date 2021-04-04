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

package org.janusgraph.graphdb.olap;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.database.StandardJanusGraph;

public class GraphProvider {

    private StandardJanusGraph graph = null;
    private boolean provided = false;

    public void setGraph(JanusGraph graph) {
        Preconditions.checkArgument(graph != null && graph.isOpen(), "Need to provide open graph");
        this.graph = (StandardJanusGraph) graph;
        provided = true;
    }

    public void initializeGraph(Configuration config) {
        if (!provided) {
            this.graph = (StandardJanusGraph) JanusGraphFactory.open((BasicConfiguration) config);
        }
    }

    public void close() {
        if (!provided && null != graph && graph.isOpen()) {
            graph.close();
            graph = null;
        }
    }

    public boolean isProvided() {
        return provided;
    }

    public final StandardJanusGraph get() {
        Preconditions.checkNotNull(graph);
        return graph;
    }
}
