package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.EdgeLabelMaker;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.core.schema.VertexLabelMaker;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.olap.computer.FulgoraGraphComputer;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.io.DefaultIo;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Blueprints specific implementation for {@link TitanGraph}.
 * Handles thread-bound transactions.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanBlueprintsGraph implements TitanGraph {

    private static final Logger log =
            LoggerFactory.getLogger(TitanBlueprintsGraph.class);


    @Override
    public Io io() {
        return new TitanIo(this);
    }

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
                "Should either open transaction or throw exception. [%s]",tinkerpopTxContainer.readWriteBehavior);
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
        return config.getLocalConfiguration();
    }

    // ########## TRANSACTIONAL FORWARDING ###########################

    @Override
    public TitanVertex addVertex(Object... keyValues) {
        return getAutoStartTx().addVertex(keyValues);
    }


    @Override
    public com.tinkerpop.gremlin.structure.Graph.Iterators iterators() {
        return getAutoStartTx().iterators();
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V(Object... ids) {
        return getAutoStartTx().V(ids);
    }

    @Override
    public GraphTraversal<Edge, Edge> E(Object... ids) {
        return getAutoStartTx().E(ids);
    }

    @Override
    public TitanGraphComputer compute(final Class... graphComputerClass) {
        GraphComputerHelper.validateComputeArguments(graphComputerClass);
        if (graphComputerClass.length == 0 || graphComputerClass[0].equals(FulgoraGraphComputer.class)) {
            StandardTitanGraph graph = (StandardTitanGraph)this;
            return new FulgoraGraphComputer(graph,graph.getConfiguration().getConfiguration());
        } else {
            throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass[0]);
        }
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
    public TitanMultiVertexQuery multiQuery(TitanVertex... vertices) {
        return getAutoStartTx().multiQuery(vertices);
    }

    @Override
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



    class GraphTransaction implements Transaction {

        private Consumer<Transaction> readWriteBehavior = READ_WRITE_BEHAVIOR.AUTO;
        private Consumer<Transaction> closeBehavior = CLOSE_BEHAVIOR.COMMIT;

        @Override
        public void open() {
            if (isOpen()) throw Exceptions.transactionAlreadyOpen();
            startNewTx();
        }

        @Override
        public void commit() {
            getAutoStartTx().commit();
        }

        @Override
        public void rollback() {
            getAutoStartTx().rollback();
        }

        @Override
        public <R> Workload<R> submit(Function<Graph, R> graphRFunction) {
            return new Workload<R>(TitanBlueprintsGraph.this,graphRFunction);
        }

        @Override
        public TitanTransaction create() {
            return newTransaction();
        }

        @Override
        public boolean isOpen() {
            TitanBlueprintsTransaction tx = txs.get();
            return tx!=null && tx.isOpen();
        }

        @Override
        public void readWrite() {
            readWriteBehavior.accept(this);
        }

        @Override
        public void close() {
            close(this);
        }

        void close(Transaction tx) {
            closeBehavior.accept(tx);
            Preconditions.checkState(!tx.isOpen(),"Invalid close behavior configured: Should close transaction. [%s]",closeBehavior);
        }

        @Override
        public Transaction onReadWrite(Consumer<Transaction> transactionConsumer) {
            if (transactionConsumer==null) throw Exceptions.onReadWriteBehaviorCannotBeNull();
            Preconditions.checkArgument(transactionConsumer instanceof READ_WRITE_BEHAVIOR,
                    "Only READ_WRITE_BEHAVIOR instances are accepted argument, got: %s", transactionConsumer);
            this.readWriteBehavior = transactionConsumer;
            return this;
        }

        @Override
        public Transaction onClose(Consumer<Transaction> transactionConsumer) {
            if (transactionConsumer==null) throw Exceptions.onCloseBehaviorCannotBeNull();
            Preconditions.checkArgument(transactionConsumer instanceof CLOSE_BEHAVIOR,
                    "Only CLOSE_BEHAVIOR instances are accepted argument, got: %s", transactionConsumer);
            this.closeBehavior = transactionConsumer;
            return this;
        }
    }

}
