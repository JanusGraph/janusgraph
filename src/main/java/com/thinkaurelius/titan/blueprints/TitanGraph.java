package com.thinkaurelius.titan.blueprints;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.thinkaurelius.titan.blueprints.util.TitanEdgeSequence;
import com.thinkaurelius.titan.blueprints.util.TitanVertexSequence;
import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.exceptions.InvalidNodeException;
import com.tinkerpop.blueprints.pgm.*;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.impls.Parameter;

import java.util.*;

public class TitanGraph implements TransactionalGraph, IndexableGraph {

    private static final String INTERNAL_PREFIX = "#!bp!#";
    private static final String indexNameProperty = INTERNAL_PREFIX + "indexname";
    private static final String globalTagProperty = INTERNAL_PREFIX + "tag";
    private static final String indexTag = "index";

    private final GraphDatabase db;

    private static final ThreadLocal<GraphTransaction> txs =  new ThreadLocal<GraphTransaction>() {

        protected GraphTransaction initialValue() {
            return null;
        }

        protected void finalize() throws Throwable {
            GraphTransaction tx = get();
            if (tx!=null && tx.isOpen()) {
                tx.commit();
                openTx.remove(tx);
            }
            remove();
            super.finalize();
        }

    };

    private static final WeakHashMap<GraphTransaction,Boolean> openTx = new WeakHashMap<GraphTransaction, Boolean>(4);

    
    public TitanGraph(final String directory) {
        this(new GraphDatabaseConfiguration(directory));
    }
    
    public TitanGraph(final GraphDatabaseConfiguration config) {
        this(config.openDatabase());
    }
    
    public TitanGraph(final GraphDatabase db) {
        this.db=db;
        //Verify that database has been setup correctly
        GraphTransaction tx = db.startTransaction();
        if (!tx.containsEdgeType(globalTagProperty)) {
            //Create initial state
            tx.createEdgeType().withName(indexNameProperty).
                    category(EdgeCategory.Simple).functional(true).
                    setIndex(PropertyIndex.Standard).makeKeyed().
                    dataType(String.class).makePropertyType();
            tx.createEdgeType().withName(globalTagProperty).
                    category(EdgeCategory.Simple).
                    setIndex(PropertyIndex.Standard).
                    dataType(String.class).makePropertyType();
        }
        tx.commit();
    }


    @Override
    public Vertex addVertex(final Object id) {
        return new TitanVertex(getAutoStartTx().createNode());
    }

    @Override
    public Vertex getVertex(final Object id) {
        GraphTransaction tx = getAutoStartTx();
        if (null == id)
            throw new IllegalArgumentException("Element identifier cannot be null");

        try {
            final Long longId;
            if (id instanceof Long)
                longId = (Long) id;
            else
                longId = Double.valueOf(id.toString()).longValue();
            Node node = tx.getNode(longId);
            return new TitanVertex(node);
        } catch (InvalidNodeException e) {
            return null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public void removeVertex(final Vertex vertex) {
        ((TitanVertex)vertex).getRawElement().delete();
    }

    @Override
    public Iterable<Vertex> getVertices() {
        GraphTransaction tx = getAutoStartTx();
        return new TitanVertexSequence(tx.getAllNodes());
    }

    @Override
    public Edge addEdge(final Object id, final Vertex outVertex, final Vertex inVertex, final String label) {
        if (label.startsWith(INTERNAL_PREFIX)) throw new IllegalArgumentException("Edge labels cannot start with prefix " + INTERNAL_PREFIX);
        Node startNode = ((TitanVertex)outVertex).getRawElement();
        Node endNode = ((TitanVertex)inVertex).getRawElement();
        Relationship r = getAutoStartTx().createRelationship(label,startNode,endNode);
        return new TitanEdge(r);
    }

    @Override
    public Edge getEdge(final Object id) {
        throw new UnsupportedOperationException("Edges cannot be retrieved by ID.");
    }

    @Override
    public void removeEdge(final Edge edge) {
        ((TitanEdge)edge).getRawElement().delete();
    }

    @Override
    public Iterable<Edge> getEdges() {
        GraphTransaction tx = getAutoStartTx();
        return new TitanEdgeSequence(tx.getAllRelationships());
    }

    @Override
    public void clear() {
        GraphTransaction tx = getAutoStartTx();
        List<Node> nodes = new ArrayList<Node>();
        for (Node node : tx.getAllNodes()) {
            Iterator<com.thinkaurelius.titan.core.Edge> iter = node.getEdgeIterator();
            while (iter.hasNext()) {
                com.thinkaurelius.titan.core.Edge e = iter.next();
                iter.remove();
            }
            nodes.add(node);
        }
        for (Node node : nodes) node.delete();
    }



    /** ==================== Indexable Graph ================= **/

    @Override
    public <T extends Element> Index<T> createManualIndex(final String indexName, final Class<T> indexClass, final Parameter... indexParameters) {
        throw new UnsupportedOperationException("Manual indexes are not supported by Titan");
    }

    @Override
    public <T extends Element> AutomaticIndex<T> createAutomaticIndex(final String indexName, final Class<T> indexClass, final Set<String> autoIndexKeys, final Parameter... parameters) {
        Preconditions.checkNotNull(indexName);
        Preconditions.checkArgument(autoIndexKeys!=null && !autoIndexKeys.isEmpty());
        Preconditions.checkArgument(indexClass.equals(TitanVertex.class),"Can only index vertices");
        Preconditions.checkArgument(parameters.length==1,"Expected class parameter");
        Parameter<String,Class<?>> clazz = parameters[0];
        
        GraphTransaction tx = db.startTransaction();
        try {
        
            for (String key : autoIndexKeys) {
                PropertyType t= tx.createEdgeType().withName(indexName).
                        category(EdgeCategory.Simple).functional(true).
                        setIndex(PropertyIndex.Standard).
                        dataType(clazz.getValue()).makePropertyType();
                t.createProperty(indexNameProperty,indexName);
                t.createProperty(globalTagProperty,indexTag);
            }
            tx.commit();
        } finally {
            if (tx.isOpen()) tx.abort();
        }

        return new TitanIndex(this,indexName,autoIndexKeys,TitanVertex.class);
    }

    @Override
    public <T extends Element> Index<T> getIndex(final String indexName, final Class<T> indexClass) {
        Set<String> keys = new HashSet<String>();
        for (Node node : indexRetrieval(indexNameProperty,indexName)) {
            Preconditions.checkArgument(node instanceof PropertyType);
            keys.add(((PropertyType)node).getName());
        }
        if (keys.isEmpty()) throw new IllegalArgumentException("Unknown index: " + indexName);
        else return new TitanIndex(this,indexName,keys,TitanVertex.class);
    }

    @Override
    public Iterable<Index<? extends Element>> getIndices() {
        HashMultimap<String,String> properties = HashMultimap.create();
        for (Node node : indexRetrieval(globalTagProperty,indexTag)) {
            Preconditions.checkArgument(node instanceof PropertyType);
            PropertyType pt = (PropertyType)node;
            String indexName = pt.getString(indexNameProperty);
            properties.put(indexName,pt.getName());
        }
        List<Index<? extends Element>> indexes = new ArrayList<Index<? extends Element>>();
        for (String indexName : properties.keySet()) {
            indexes.add(new TitanIndex(this,indexName,properties.get(indexName),TitanVertex.class));
        }
        return indexes;
    }

    @Override
    public void dropIndex(String s) {
        throw new UnsupportedOperationException("Dropping indexes is not supported!");
    }

    Set<Node> indexRetrieval(String type, Object attribute) {
        return getAutoStartTx().getNodesByAttribute(type,attribute);
    }

    /** ==================== Transactional Graph ================= **/

    @Override
    public void setMaxBufferSize(final int i) {
        if (i!=0) throw new UnsupportedOperationException("Does not support buffer size other than 0");
    }

    @Override
    public int getMaxBufferSize() {
        return 0;
    }

    @Override
    public int getCurrentBufferSize() {
        return 0;
    }


    @Override
    public void stopTransaction(final Conclusion conclusion) {
        GraphTransaction tx = txs.get();
        if (tx==null || tx.isClosed()) throw new IllegalStateException("A transaction has not yet been started.");
        switch(conclusion) {
            case SUCCESS: tx.commit(); break;
            case FAILURE: tx.abort(); break;
            default: throw new AssertionError("Unrecognized conclusion: " + conclusion);
        }
        txs.remove();
        openTx.remove(tx);
    }

    private GraphTransaction internalStartTransaction() {
        GraphTransaction tx = db.startTransaction();
        txs.set(tx);
        openTx.put(tx,Boolean.TRUE);
        return tx;
    }

    @Override
    public void startTransaction() {
        getAutoStartTx();
    }
    
    private GraphTransaction getAutoStartTx() {
        GraphTransaction tx = txs.get();
        if (tx==null || tx.isClosed()) {
            internalStartTransaction();
        }
        return tx;
    }


    @Override
    public void shutdown() {
        if (txs.get()!=null) stopTransaction(Conclusion.SUCCESS);
        for (GraphTransaction tx : openTx.keySet()) {
            tx.commit();
        }
        db.close();
    }


}
