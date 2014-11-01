package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.EdgeLabelMaker;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.core.schema.VertexLabelMaker;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.*;
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

    // ########## TRANSACTION HANDLING ###########################

    final GraphTransaction tinkerpopTxContainer = new GraphTransaction();

    private ThreadLocal<TitanBlueprintsTransaction> txs = new ThreadLocal<TitanBlueprintsTransaction>() {

        protected TitanBlueprintsTransaction initialValue() {
            return null;
        }

    };

    /**
     * ThreadLocal transactions used behind the scenes in
     * {@link TransactionalGraph} methods. Transactions started through
     * {@code ThreadedTransactionalGraph#newTransaction()} aren't included in
     * this map. Contrary to the javadoc comment above
     * {@code ThreadedTransactionalGraph#newTransaction()}, the caller is
     * responsible for holding references to and committing or rolling back any
     * transactions started through
     * {@code ThreadedTransactionalGraph#newTransaction()}.
     */
    private final Map<TitanBlueprintsTransaction, Boolean> openTx =
            new ConcurrentHashMap<TitanBlueprintsTransaction, Boolean>();


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
        openTx.put(tx, Boolean.TRUE);
        log.debug("Created new thread-bound transaction {}", tx);
        return tx;
    }

    public TitanTransaction getCurrentThreadTx() {
        return getAutoStartTx();
    }


    @Override
    public synchronized void close() {
        for (TitanTransaction tx : openTx.keySet()) {
            tx.close();
        }
        openTx.clear();
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
        final WriteConfiguration config = ((StandardTitanGraph)this).getBackend().getUserConfiguration();
        return new Variables() {

            @Override
            public Set<String> keys() {
                return Sets.newHashSet(config.getKeys(""));
            }

            @Override
            public <R> Optional<R> get(String s) {
                if (StringUtils.isEmpty(s)) throw Exceptions.variableKeyCanNotBeEmpty();
                Object value = config.get(s,Object.class);
                if (value==null) return Optional.empty();
                else return Optional.of((R)value);
            }

            @Override
            public void set(String s, Object o) {
                if (StringUtils.isEmpty(s)) throw Exceptions.variableKeyCanNotBeEmpty();
                if (o==null) throw Exceptions.variableValueCanNotBeNull();
                config.set(s,o);
            }

            @Override
            public void remove(String s) {
                if (StringUtils.isEmpty(s)) throw Exceptions.variableKeyCanNotBeEmpty();
                config.remove(s);
            }
        };
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
    public TitanVertex v(final Object id) {
        return getAutoStartTx().v(id);
    }

    @Override
    public TitanEdge e(final Object id) {
        return getAutoStartTx().e(id);
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V() {
        return getAutoStartTx().V();
    }

    @Override
    public GraphTraversal<Edge, Edge> E() {
        return getAutoStartTx().E();
    }

    @Override
    public <S> GraphTraversal<S, S> of() {
        return getAutoStartTx().of();
    }

    @Override
    public GraphComputer compute(final Class... graphComputerClass) {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public TitanVertex addVertex(String vertexLabel) {
        return getAutoStartTx().addVertex(vertexLabel);
    }

    @Override
    public Map<Long,TitanVertex> vertices(long... ids) {
        return getAutoStartTx().vertices(ids);
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
