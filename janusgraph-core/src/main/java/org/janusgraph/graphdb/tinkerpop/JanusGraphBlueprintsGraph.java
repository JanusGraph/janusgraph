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

package org.janusgraph.graphdb.tinkerpop;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphIndexQuery;
import org.janusgraph.core.JanusGraphMultiVertexQuery;
import org.janusgraph.core.JanusGraphQuery;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.MixedIndexCountQuery;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.EdgeLabelMaker;
import org.janusgraph.core.schema.PropertyKeyMaker;
import org.janusgraph.core.schema.VertexLabelMaker;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.olap.computer.FulgoraGraphComputer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Blueprints specific implementation for {@link JanusGraph}.
 * Handles thread-bound transactions.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class JanusGraphBlueprintsGraph implements JanusGraph {

    private static final Logger log =
            LoggerFactory.getLogger(JanusGraphBlueprintsGraph.class);




    // ########## TRANSACTION HANDLING ###########################

    final GraphTransaction tinkerpopTxContainer = new GraphTransaction();

    private ThreadLocal<JanusGraphBlueprintsTransaction> txs = ThreadLocal.withInitial(() -> null);

    public abstract JanusGraphTransaction newThreadBoundTransaction();

    private JanusGraphBlueprintsTransaction getAutoStartTx() {
        if (txs == null) throw new IllegalStateException("Graph has been closed");
        tinkerpopTxContainer.readWrite();

        JanusGraphBlueprintsTransaction tx = txs.get();
        Preconditions.checkNotNull(tx,"Invalid read-write behavior configured: " +
                "Should either open transaction or throw exception.");
        return tx;
    }

    private void startNewTx() {
        JanusGraphBlueprintsTransaction tx = txs.get();
        if (tx!=null && tx.isOpen()) throw Transaction.Exceptions.transactionAlreadyOpen();
        tx = (JanusGraphBlueprintsTransaction) newThreadBoundTransaction();
        txs.set(tx);
        log.debug("Created new thread-bound transaction {}", tx);
    }

    public JanusGraphTransaction getCurrentThreadTx() {
        return getAutoStartTx();
    }


    @Override
    public synchronized void close() {
        // If user closes the graph before committing/rolling back thread-bound transactions, there could be a leakage.
        // In case that happens, here we at least remove the ThreadLocal object belonging to the current thread.
        txs.remove();
        txs = null;
    }

    @Override
    public Transaction tx() {
        return tinkerpopTxContainer;
    }

    @Override
    public String toString() {
        GraphDatabaseConfiguration config = ((StandardJanusGraph) this).getConfiguration();
        return StringFactory.graphString(this,config.getBackendDescription());
    }

    @Override
    public Variables variables() {
        return new JanusGraphVariables(((StandardJanusGraph)this).getBackend().getUserConfiguration());
    }

    @Override
    public Configuration configuration() {
        GraphDatabaseConfiguration config = ((StandardJanusGraph) this).getConfiguration();
        return config.getConfigurationAtOpen();
    }

    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        if (builder.requiresVersion(GryoVersion.V1_0) || builder.requiresVersion(GraphSONVersion.V1_0)) {
            return (I) builder.graph(this).onMapper(mapper ->  mapper.addRegistry(JanusGraphIoRegistryV1d0.instance())).create();
        } else if (builder.requiresVersion(GraphSONVersion.V2_0)) {
            return (I) builder.graph(this).onMapper(mapper ->  mapper.addRegistry(JanusGraphIoRegistry.instance())).create();
        } else {
            return (I) builder.graph(this).onMapper(mapper ->  mapper.addRegistry(JanusGraphIoRegistry.instance())).create();
        }
    }

    // ########## TRANSACTIONAL FORWARDING ###########################

    @Override
    public JanusGraphVertex addVertex(Object... keyValues) {
        return getAutoStartTx().addVertex(keyValues);
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        return getAutoStartTx().vertices(vertexIds);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        return getAutoStartTx().edges(edgeIds);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        if (!graphComputerClass.equals(FulgoraGraphComputer.class)) {
            throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass);
        } else {
            return (C)compute();
        }
    }

    @Override
    public FulgoraGraphComputer compute() throws IllegalArgumentException {
        StandardJanusGraph graph = (StandardJanusGraph)this;
        return new FulgoraGraphComputer(graph,graph.getConfiguration().getConfiguration());
    }

    @Override
    public JanusGraphVertex addVertex(String vertexLabel) {
        return getAutoStartTx().addVertex(vertexLabel);
    }

    @Override
    public JanusGraphQuery<? extends JanusGraphQuery> query() {
        return getAutoStartTx().query();
    }

    @Override
    public MixedIndexCountQuery mixedIndexCountQuery() {
        return getAutoStartTx().mixedIndexCountQuery();
    }

    @Override
    public JanusGraphIndexQuery indexQuery(String indexName, String query) {
        return getAutoStartTx().indexQuery(indexName,query);
    }

    @Override
    public JanusGraphMultiVertexQuery multiQuery(JanusGraphVertex... vertices) {
        return getAutoStartTx().multiQuery(vertices);
    }

    @Override
    public JanusGraphMultiVertexQuery multiQuery(Collection<JanusGraphVertex> vertices) {
        return getAutoStartTx().multiQuery(vertices);
    }


    //Schema

    @Override
    public PropertyKeyMaker makePropertyKey(String name) {
        return getAutoStartTx().makePropertyKey(name);
    }

    @Override
    public EdgeLabelMaker makeEdgeLabel(String name) {
        return getAutoStartTx().makeEdgeLabel(name);
    }

    @Override
    public VertexLabelMaker makeVertexLabel(String name) {
        return getAutoStartTx().makeVertexLabel(name);
    }

    @Override
    public VertexLabel addProperties(VertexLabel vertexLabel, PropertyKey... keys) {
        return getAutoStartTx().addProperties(vertexLabel, keys);
    }

    @Override
    public EdgeLabel addProperties(EdgeLabel edgeLabel, PropertyKey... keys) {
        return getAutoStartTx().addProperties(edgeLabel, keys);
    }

    @Override
    public EdgeLabel addConnection(EdgeLabel edgeLabel, VertexLabel outVLabel, VertexLabel inVLabel) {
        return getAutoStartTx().addConnection(edgeLabel, outVLabel, inVLabel);
    }

    @Override
    public boolean containsPropertyKey(String name) {
        return getAutoStartTx().containsPropertyKey(name);
    }

    @Override
    public PropertyKey getOrCreatePropertyKey(String name) {
        return getAutoStartTx().getOrCreatePropertyKey(name);
    }

    @Override
    public PropertyKey getPropertyKey(String name) {
        return getAutoStartTx().getPropertyKey(name);
    }

    @Override
    public boolean containsEdgeLabel(String name) {
        return getAutoStartTx().containsEdgeLabel(name);
    }

    @Override
    public EdgeLabel getOrCreateEdgeLabel(String name) {
        return getAutoStartTx().getOrCreateEdgeLabel(name);
    }

    @Override
    public EdgeLabel getEdgeLabel(String name) {
        return getAutoStartTx().getEdgeLabel(name);
    }

    @Override
    public boolean containsRelationType(String name) {
        return getAutoStartTx().containsRelationType(name);
    }

    @Override
    public RelationType getRelationType(String name) {
        return getAutoStartTx().getRelationType(name);
    }

    @Override
    public boolean containsVertexLabel(String name) {
        return getAutoStartTx().containsVertexLabel(name);
    }

    @Override
    public VertexLabel getVertexLabel(String name) {
        return getAutoStartTx().getVertexLabel(name);
    }

    @Override
    public VertexLabel getOrCreateVertexLabel(String name) {
        return getAutoStartTx().getOrCreateVertexLabel(name);
    }



    class GraphTransaction extends AbstractThreadLocalTransaction {

        public GraphTransaction() {
            super(JanusGraphBlueprintsGraph.this);
        }

        @Override
        public void commit() {
            try {
                super.commit();
            } finally {
                doClose();
            }
        }

        @Override
        public void rollback() {
            try {
                super.rollback();
            } finally {
                doClose();
            }
        }

        @Override
        public void doOpen() {
            startNewTx();
        }

        @Override
        public void doCommit() {
            getAutoStartTx().commit();
        }

        @Override
        public void doRollback() {
            getAutoStartTx().rollback();
        }

        @Override
        public JanusGraphTransaction createThreadedTx() {
            return newTransaction();
        }

        @Override
        public boolean isOpen() {
            if (null == txs) {
                // Graph has been closed
                return false;
            }
            JanusGraphBlueprintsTransaction tx = txs.get();
            return tx!=null && tx.isOpen();
        }

        @Override
        protected void doClose() {
            super.doClose();
            transactionListeners.remove();
            txs.remove();
        }

        @Override
        public Transaction onReadWrite(Consumer<Transaction> transactionConsumer) {
            Preconditions.checkArgument(transactionConsumer instanceof READ_WRITE_BEHAVIOR,
                    "Only READ_WRITE_BEHAVIOR instances are accepted argument, got: %s", transactionConsumer);
            return super.onReadWrite(transactionConsumer);
        }

        @Override
        public Transaction onClose(Consumer<Transaction> transactionConsumer) {
            Preconditions.checkArgument(transactionConsumer instanceof CLOSE_BEHAVIOR,
                    "Only CLOSE_BEHAVIOR instances are accepted argument, got: %s", transactionConsumer);
            return super.onClose(transactionConsumer);
        }
    }

}
