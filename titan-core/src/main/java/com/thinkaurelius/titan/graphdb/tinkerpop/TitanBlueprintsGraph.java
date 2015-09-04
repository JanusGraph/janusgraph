package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.EdgeLabelMaker;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.core.schema.VertexLabelMaker;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.olap.computer.FulgoraGraphComputer;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Blueprints specific implementation for {@link TitanGraph}.
 * Handles thread-bound transactions.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanBlueprintsGraph implements TitanGraph {

    private static final Logger log =
            LoggerFactory.getLogger(TitanBlueprintsGraph.class);




    // ########## TRANSACTION HANDLING ###########################

    final GraphTransaction tinkerpopTxContainer = new GraphTransaction();

    private ThreadLocal<TitanBlueprintsTransaction> txs = new ThreadLocal<TitanBlueprintsTransaction>() {

        protected TitanBlueprintsTransaction initialValue() {
            return null;
        }

    };

    public abstract TitanTransaction newThreadBoundTransaction();

    private TitanBlueprintsTransaction getAutoStartTx() {
        if (txs == null) throw new IllegalStateException("Graph has been closed");
        tinkerpopTxContainer.readWrite();

        TitanBlueprintsTransaction tx = txs.get();
        Preconditions.checkState(tx!=null,"Invalid read-write behavior configured: " +
                "Should either open transaction or throw exception.");
        return tx;
    }

    private TitanBlueprintsTransaction startNewTx() {
        TitanBlueprintsTransaction tx = txs.get();
        if (tx!=null && tx.isOpen()) throw Transaction.Exceptions.transactionAlreadyOpen();
        tx = (TitanBlueprintsTransaction) newThreadBoundTransaction();
        txs.set(tx);
        log.debug("Created new thread-bound transaction {}", tx);
        return tx;
    }

    public TitanTransaction getCurrentThreadTx() {
        return getAutoStartTx();
    }


    @Override
    public synchronized void close() {
        txs = null;
    }

    @Override
    public Transaction tx() {
        return tinkerpopTxContainer;
    }

    @Override
    public String toString() {
        GraphDatabaseConfiguration config = ((StandardTitanGraph) this).getConfiguration();
        return StringFactory.graphString(this,config.getBackendDescription());
    }

    @Override
    public Variables variables() {
        return new TitanGraphVariables(((StandardTitanGraph)this).getBackend().getUserConfiguration());
    }

    @Override
    public Configuration configuration() {
        GraphDatabaseConfiguration config = ((StandardTitanGraph) this).getConfiguration();
        return config.getConfigurationAtOpen();
    }

    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        return (I) builder.graph(this).registry(TitanIoRegistry.INSTANCE).create();
    }

    // ########## TRANSACTIONAL FORWARDING ###########################

    @Override
    public TitanVertex addVertex(Object... keyValues) {
        return getAutoStartTx().addVertex(keyValues);
    }

//    @Override
//    public com.tinkerpop.gremlin.structure.Graph.Iterators iterators() {
//        return getAutoStartTx().iterators();
//    }

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
        StandardTitanGraph graph = (StandardTitanGraph)this;
        return new FulgoraGraphComputer(graph,graph.getConfiguration().getConfiguration());
    }

    @Override
    public TitanVertex addVertex(String vertexLabel) {
        return getAutoStartTx().addVertex(vertexLabel);
    }

    @Override
    public TitanGraphQuery<? extends TitanGraphQuery> query() {
        return getAutoStartTx().query();
    }

    @Override
    public TitanIndexQuery indexQuery(String indexName, String query) {
        return getAutoStartTx().indexQuery(indexName,query);
    }

    @Override
    @Deprecated
    public TitanMultiVertexQuery multiQuery(TitanVertex... vertices) {
        return getAutoStartTx().multiQuery(vertices);
    }

    @Override
    @Deprecated
    public TitanMultiVertexQuery multiQuery(Collection<TitanVertex> vertices) {
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



    class GraphTransaction extends AbstractTransaction {

        public GraphTransaction() {
            super(TitanBlueprintsGraph.this);
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
        public TitanTransaction createThreadedTx() {
            return newTransaction();
        }

        @Override
        public boolean isOpen() {
            TitanBlueprintsTransaction tx = txs.get();
            return tx!=null && tx.isOpen();
        }

        @Override
        public void close() {
            close(this);
        }

        void close(Transaction tx) {
            closeConsumer.accept(tx);
            Preconditions.checkState(!tx.isOpen(),"Invalid close behavior configured: Should close transaction. [%s]",closeConsumer);
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
