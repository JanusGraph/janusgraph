// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.optimize.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.vertices.AbstractVertex;

import java.util.List;

/**
 * This strategy is to ensure traversals work properly even if it encounters proxy nodes
 */
public class JanusGraphProxyTraversalStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final JanusGraphProxyTraversalStrategy INSTANCE = new JanusGraphProxyTraversalStrategy();

    private JanusGraphProxyTraversalStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(EdgeOtherVertexStep.class, traversal).forEach(originalStep -> {
            final JanusGraphEdgeOtherVertexStep step = new JanusGraphEdgeOtherVertexStep(traversal);
            TraversalHelper.replaceStep(originalStep, step, traversal);
        });
    }

    public static JanusGraphProxyTraversalStrategy instance() {
        return INSTANCE;
    }

    public static class JanusGraphEdgeOtherVertexStep extends EdgeOtherVertexStep {

        private StandardJanusGraph janusGraph;

        public JanusGraphEdgeOtherVertexStep(final Traversal.Admin<?, ?> traversal) {
            super(traversal);
            final Graph graph = traversal.getGraph().get();
            janusGraph = graph instanceof StandardJanusGraphTx ? ((StandardJanusGraphTx) graph).getGraph() : (StandardJanusGraph) graph;
        }

        @Override
        protected Vertex map(final Traverser.Admin<Edge> traverser) {
            final List<Object> objects = traverser.path().objects();
            for (int i = objects.size() - 2; i >= 0; i--) {
                if (objects.get(i) instanceof Vertex) {
                    final Edge edge = traverser.get();
                    final Vertex outVertex = edge.outVertex();
                    final Vertex inVertex = edge.inVertex();
                    final Vertex v = (Vertex) objects.get(i);
                    AbstractVertex otherV;
                    if (ElementHelper.areEqual(v, edge.outVertex())) {
                        otherV = (AbstractVertex) inVertex;
                    } else if (ElementHelper.areEqual(v, edge.inVertex())) {
                        otherV = (AbstractVertex) outVertex;
                    } else {
                        // at least one endpoint of this edge is a proxy node
                        if (janusGraph.getIDManager().isProxyVertex((long) outVertex.id()) && outVertex.property("canonicalId").value().equals(v.id())) {
                            otherV = (AbstractVertex) inVertex;
                        } else {
                            otherV = (AbstractVertex) outVertex;
                        }
                    }
                    if (janusGraph.getIDManager().isProxyVertex((long) otherV.id())) {
                        long canonicalId = (long) otherV.property("canonicalId").value();
                        return otherV.tx().getVertex(canonicalId);
                    } else {
                        return otherV;
                    }
                }
            }
            throw new IllegalStateException("The path history of the traverser does not contain a previous vertex: " + traverser.path());
        }

    }
}
