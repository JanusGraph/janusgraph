// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.optimize;

import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.olap.computer.FulgoraElementTraversal;
import org.janusgraph.graphdb.tinkerpop.JanusGraphBlueprintsGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;

import java.util.Optional;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphTraversalUtil {

    public static JanusGraphVertex getJanusGraphVertex(Element v) {
        while (v instanceof WrappedVertex) {
            v = ((WrappedVertex<Vertex>) v).getBaseVertex();
        }
        if (v instanceof JanusGraphVertex) {
            return (JanusGraphVertex) v;
        } else throw new IllegalArgumentException("Expected traverser of JanusGraph vertex but found: " + v);
    }

    public static JanusGraphVertex getJanusGraphVertex(Traverser<? extends Element> traverser) {
        return getJanusGraphVertex(traverser.get());
    }

    public static boolean isEdgeReturnStep(JanusGraphVertexStep vertexStep) {
        return Edge.class.isAssignableFrom(vertexStep.getReturnClass());
    }

    public static boolean isVertexReturnStep(JanusGraphVertexStep vertexStep) {
        return Vertex.class.isAssignableFrom(vertexStep.getReturnClass());
    }

    public static Step getNextNonIdentityStep(final Step start) {
        Step currentStep = start.getNextStep();
        //Skip over identity steps
        while (currentStep instanceof IdentityStep) currentStep = currentStep.getNextStep();
        return currentStep;
    }

    public static JanusGraphTransaction getTx(Traversal.Admin<?, ?> traversal) {
        final JanusGraphTransaction tx;
        Optional<Graph> optGraph = TraversalHelper.getRootTraversal(traversal.asAdmin()).getGraph();

        if (traversal instanceof FulgoraElementTraversal) {
            tx = (JanusGraphTransaction) optGraph.get();
        } else {
            if (!optGraph.isPresent())
                throw new IllegalArgumentException("Traversal is not bound to a graph: " + traversal);
            Graph graph = optGraph.get();
            if (graph instanceof JanusGraphTransaction) tx = (JanusGraphTransaction) graph;
            else if (graph instanceof JanusGraphBlueprintsGraph) tx = ((JanusGraphBlueprintsGraph) graph).getCurrentThreadTx();
            else throw new IllegalArgumentException("Traversal is not bound to a JanusGraph Graph, but: " + graph);
        }
        if (tx == null)
            throw new IllegalArgumentException("Not a valid start step for a JanusGraph traversal: " + traversal);
        if (tx.isOpen()) return tx;
        else return ((StandardJanusGraphTx) tx).getNextTx();
    }

}
