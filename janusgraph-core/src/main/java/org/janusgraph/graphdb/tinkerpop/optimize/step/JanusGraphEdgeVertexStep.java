// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.optimize.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphMultiVertexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.janusgraph.graphdb.tinkerpop.profile.TP3ProfileWrapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A JanusGraphEdgeVertexStep is identical to a {@link EdgeVertexStep}. The only difference
 * being that it can use multiQuery to pre-fetch the vertex properties prior to the execution
 * of any subsequent has steps and so eliminate the need for a network trip for each vertex.
 * It implements the optimisation enabled via the query.batch-property-prefetch config option.
 */
public class JanusGraphEdgeVertexStep extends EdgeVertexStep implements Profiling {

    private boolean initialized = false;
    private QueryProfiler queryProfiler = QueryProfiler.NO_OP;
    private final int txVertexCacheSize;

    public JanusGraphEdgeVertexStep(EdgeVertexStep originalStep, int txVertexCacheSize) {
        super(originalStep.getTraversal(), originalStep.getDirection());
        originalStep.getLabels().forEach(this::addLabel);
        this.txVertexCacheSize = txVertexCacheSize;
    }

    private void initialize() {
        assert !initialized;
        initialized = true;

        if (!starts.hasNext()) {
            throw FastNoSuchElementException.instance();
        }

        if(txVertexCacheSize < 2){
            return;
        }

        List<Traverser.Admin<Edge>> edges = new ArrayList<>();
        Set<Vertex> vertices = new HashSet<>();

        do{
            Traverser.Admin<Edge> e = starts.next();
            edges.add(e);

            if(vertices.size() < txVertexCacheSize){
                if(Direction.IN.equals(direction)){
                    vertices.add(e.get().inVertex());
                } else if(Direction.OUT.equals(direction)){
                    vertices.add(e.get().outVertex());
                } else if(Direction.BOTH.equals(direction)){
                    vertices.add(e.get().inVertex());
                    vertices.add(e.get().outVertex());
                }
            }

        } while (starts.hasNext());

        // If there are multiple vertices then fetch the properties for all of them in a single multiQuery to
        // populate the vertex cache so subsequent queries of properties don't have to go to the storage back end
        if (vertices.size() > 1) {
            JanusGraphMultiVertexQuery multiQuery = JanusGraphTraversalUtil.getTx(traversal).multiQuery();
            ((BasicVertexCentricQueryBuilder) multiQuery).profiler(queryProfiler);
            multiQuery.addAllVertices(vertices).preFetch();
        }

        starts.add(edges.iterator());
    }

    @Override
    protected Traverser.Admin<Vertex> processNextStart() {
        if (!initialized) initialize();
        return super.processNextStart();
    }

    @Override
    public void reset() {
        super.reset();
        this.initialized = false;
    }

    @Override
    public JanusGraphEdgeVertexStep clone() {
        final JanusGraphEdgeVertexStep clone = (JanusGraphEdgeVertexStep) super.clone();
        clone.initialized = false;
        return clone;
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        queryProfiler = new TP3ProfileWrapper(metrics);
    }
}
