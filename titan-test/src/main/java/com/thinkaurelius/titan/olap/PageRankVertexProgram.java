package com.thinkaurelius.titan.olap;

import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.commons.configuration.Configuration;

import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

/**
 * This implementation is only intended for testing.
 * <p>
 * Limitations:
 * <p>
 * <ul>
 * <li>Ignores edge labels</li>
 * <li>Assumes there is at most one edge (of any label) between any vertex pair</li>
 * </ul>
 * This is almost an exact copy of the TinkerPop 3 PR implementation.
 */
public class PageRankVertexProgram extends StaticVertexProgram<Double> {


    public static final String PAGE_RANK = "titan.pageRank.pageRank";
    public static final String OUTGOING_EDGE_COUNT = "titan.pageRank.edgeCount";

    private static final String DAMPING_FACTOR = "titan.pageRank.dampingFactor";
    private static final String MAX_ITERATIONS = "titan.pageRank.maxIterations";
    private static final String VERTEX_COUNT = "titan.pageRank.vertexCount";

    private double dampingFactor;
    private int maxIterations;
    private long vertexCount;

    private MessageScope.Local<Double> outE = MessageScope.Local.of(__::outE);
    private MessageScope.Local<Double> inE = MessageScope.Local.of(__::inE);

    private static final Set<VertexComputeKey> COMPUTE_KEYS = ImmutableSet.of(VertexComputeKey.of(PAGE_RANK, false), VertexComputeKey.of(OUTGOING_EDGE_COUNT, false));

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        dampingFactor = configuration.getDouble(DAMPING_FACTOR, 0.85D);
        maxIterations = configuration.getInt(MAX_ITERATIONS, 10);
        vertexCount = configuration.getLong(VERTEX_COUNT, 1L);
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(VERTEX_PROGRAM, PageRankVertexProgram.class.getName());
        configuration.setProperty(DAMPING_FACTOR, dampingFactor);
        configuration.setProperty(MAX_ITERATIONS, maxIterations);
        configuration.setProperty(VERTEX_COUNT, vertexCount);
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return COMPUTE_KEYS;
    }

    @Override
    public void setup(Memory memory) {
    }

    @Override
    public void execute(Vertex vertex, Messenger<Double> messenger, Memory memory) {
        if (memory.isInitialIteration()) {
            messenger.sendMessage(inE, 1D);
        } else if (1 == memory.getIteration()) {
            double initialPageRank = 1D / vertexCount;
            double edgeCount = IteratorUtils.stream(messenger.receiveMessages()).reduce(0D, (a, b) -> a + b);
            vertex.property(VertexProperty.Cardinality.single, PAGE_RANK, initialPageRank);
            vertex.property(VertexProperty.Cardinality.single, OUTGOING_EDGE_COUNT, edgeCount);
            messenger.sendMessage(outE, initialPageRank / edgeCount);
        } else {
            double newPageRank = IteratorUtils.stream(messenger.receiveMessages()).reduce(0D, (a, b) -> a + b);
            newPageRank =  (dampingFactor * newPageRank) + ((1D - dampingFactor) / vertexCount);
            vertex.property(VertexProperty.Cardinality.single, PAGE_RANK, newPageRank);
            messenger.sendMessage(outE, newPageRank / vertex.<Double>value(OUTGOING_EDGE_COUNT));
        }
    }

    @Override
    public boolean terminate(Memory memory) {
        return memory.getIteration() >= maxIterations;
    }

    @Override
    public Set<MessageScope> getMessageScopes(Memory memory) {
        return ImmutableSet.of(outE, inE);
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.ORIGINAL;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.VERTEX_PROPERTIES;
    }

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresLocalMessageScopes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }
        };
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractVertexProgramBuilder<Builder> {

        private Builder() {
            super(PageRankVertexProgram.class);
        }

        public Builder vertexCount(final long vertexCount) {
            configuration.setProperty(VERTEX_COUNT, vertexCount);
            return this;
        }

        public Builder dampingFactor(final double dampingFactor) {
            configuration.setProperty(DAMPING_FACTOR, dampingFactor);
            return this;
        }

        public Builder iterations(final int iterations) {
            configuration.setProperty(MAX_ITERATIONS, iterations);
            return this;
        }
    }
}
