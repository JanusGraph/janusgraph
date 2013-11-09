package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.util.ExceptionFactory;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

    @Override
    public void commit() {
        TitanTransaction tx = txs.get();
        if (tx != null && tx.isOpen()) {
            try {
                tx.commit();
            } finally {
                txs.remove();
                openTx.remove(tx);
                log.debug("Committed thread-bound transaction {}", tx);
            }
        }
    }

    @Override
    public void rollback() {
        TitanTransaction tx = txs.get();
        if (tx != null && tx.isOpen()) {
            try {
                tx.rollback();
            } finally {
                txs.remove();
                openTx.remove(tx);
                log.debug("Rolled back thread-bound transaction {}", tx);
            }
        }
    }

    @Override
    @Deprecated
    public void stopTransaction(Conclusion conclusion) {
        switch (conclusion) {
            case SUCCESS:
                commit();
                break;
            case FAILURE:
                rollback();
                break;
            default:
                throw new IllegalArgumentException("Unrecognized conclusion: " + conclusion);
        }
    }

    public abstract TitanTransaction newThreadBoundTransaction();

    private TitanBlueprintsTransaction getAutoStartTx() {
        if (txs == null) ExceptionFactory.graphShutdown();
        TitanBlueprintsTransaction tx = txs.get();
        if (tx == null) {
            tx = (TitanBlueprintsTransaction) newThreadBoundTransaction();
            txs.set(tx);
            openTx.put(tx, Boolean.TRUE);
            log.debug("Created new thread-bound transaction {}", tx);
        }
        return tx;
    }

    public TitanTransaction getCurrentThreadTx() {
        return getAutoStartTx();
    }


    @Override
    public synchronized void shutdown() {
        for (TitanTransaction tx : openTx.keySet()) {
            tx.commit();
        }
        openTx.clear();
        txs = null;
    }

    @Override
    public String toString() {
        GraphDatabaseConfiguration config = ((StandardTitanGraph) this).getConfiguration();
        return "titangraph" + StringFactory.L_BRACKET +
                config.getBackendDescription() + StringFactory.R_BRACKET;
//        return StringFactory.graphString(this,config.getBackendDescription());
    }

    @Override
    public <T extends TitanType> Iterable<T> getTypes(Class<T> clazz) {
        return getAutoStartTx().getTypes(clazz);
    }

    // ########## INDEX HANDLING ###########################

    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
        throw new UnsupportedOperationException("Key indexes cannot be dropped");
    }

    @Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, final Parameter... indexParameters) {
        getAutoStartTx().createKeyIndex(key, elementClass);
    }

    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        return getAutoStartTx().getIndexedKeys(elementClass);
    }

    // ########## TRANSACTIONAL FORWARDING ###########################

    @Override
    public TitanVertex addVertex(Object id) {
        return getAutoStartTx().addVertex(id);
    }

    @Override
    public TitanVertex getVertex(Object id) {
        return getAutoStartTx().getVertex(id);
    }

    @Override
    public void removeVertex(Vertex vertex) {
        getAutoStartTx().removeVertex(vertex);
    }

    @Override
    public Iterable<Vertex> getVertices() {
        return getAutoStartTx().getVertices();
    }

    @Override
    public KeyMaker makeKey(String name) {
        return getAutoStartTx().makeKey(name);
    }

    @Override
    public LabelMaker makeLabel(String name) {
        return getAutoStartTx().makeLabel(name);
    }

    @Override
    public TitanGraphQuery query() {
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

    @Override
    public TitanType getType(String name) {
        return getAutoStartTx().getType(name);
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        return getAutoStartTx().getVertices(key, value);
    }

    @Override
    public TitanEdge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        return getAutoStartTx().addEdge(id, outVertex, inVertex, label);
    }

    @Override
    public TitanEdge getEdge(Object id) {
        return getAutoStartTx().getEdge(id);
    }

    @Override
    public void removeEdge(Edge edge) {
        getAutoStartTx().removeEdge(edge);
    }

    @Override
    public Iterable<Edge> getEdges() {
        return getAutoStartTx().getEdges();
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        return getAutoStartTx().getEdges(key, value);
    }

}
