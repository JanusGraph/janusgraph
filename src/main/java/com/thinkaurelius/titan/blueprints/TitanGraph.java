package com.thinkaurelius.titan.blueprints;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.thinkaurelius.titan.blueprints.util.TitanEdgeSequence;
import com.thinkaurelius.titan.blueprints.util.TitanVertexSequence;
import com.thinkaurelius.titan.blueprints.util.TransactionWrapper;
import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.exceptions.InvalidNodeException;
import com.tinkerpop.blueprints.pgm.*;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.impls.Parameter;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;

import java.util.*;

public class TitanGraph implements TransactionalGraph, IndexableGraph {

    private static final String INTERNAL_PREFIX = "#!bp!#";
    
    private static final String indexNameProperty = INTERNAL_PREFIX + "indexname";
    private static final String globalTagProperty = INTERNAL_PREFIX + "tag";
    private static final String indexEverythingProperty = INTERNAL_PREFIX + "indexeverything";
    
    private static final String indexTag = "index";

    private final GraphDatabase db;
    private int bufferSize = 0;
    
    private boolean indexEveryProperty;

    private final ThreadLocal<TransactionWrapper> txs =  new ThreadLocal<TransactionWrapper>() {

        protected TransactionWrapper initialValue() {
            return null;
        }

        protected void finalize() throws Throwable {
            TransactionWrapper txw = get();
            if (txw!=null) {
                GraphTransaction tx = txw.getTransaction();
                tx.commit();
                openTx.remove(tx);
            }
            remove();
            super.finalize();
        }

    };

    private final WeakHashMap<GraphTransaction,Boolean> openTx = new WeakHashMap<GraphTransaction, Boolean>(4);

    
    public TitanGraph(String directory) {
        this(new GraphDatabaseConfiguration(directory));
    }
    
    public TitanGraph(GraphDatabaseConfiguration config) {
        this(config.openDatabase());
    }
    
    public TitanGraph(GraphDatabase db) {
        this.db=db;
        //Verify that database has been setup correctly
        GraphTransaction tx = db.startTransaction();
        if (!tx.containsEdgeType(indexNameProperty)) {
            //Create initial state
            PropertyType idx = tx.createEdgeType().withName(indexNameProperty).
                    category(EdgeCategory.Simple).
                    setIndex(PropertyIndex.Standard).
                    dataType(String.class).makePropertyType();
            tx.createEdgeType().withName(globalTagProperty).
                    category(EdgeCategory.Simple).
                    setIndex(PropertyIndex.Standard).
                    dataType(String.class).makePropertyType();
            PropertyType idxevery = tx.createEdgeType().withName(indexEverythingProperty).
                    category(EdgeCategory.Simple).
                    setIndex(PropertyIndex.None).
                    dataType(Boolean.class).makePropertyType();
            indexEveryProperty = true;
            idx.createProperty(idxevery,indexEveryProperty);
        } else {
            indexEveryProperty = tx.getPropertyType(indexNameProperty).getAttribute(indexEverythingProperty,Boolean.class).booleanValue();
        }
        tx.commit();
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this,"Titan");
    }

    @Override
    public Vertex addVertex(Object id) {
        TitanVertex v = new TitanVertex(getAutoStartTx().createNode(),this);
        return v;
    }

    @Override
    public Vertex getVertex(Object id) {
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
            return new TitanVertex(node,this);
        } catch (InvalidNodeException e) {
            return null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public void removeVertex(Vertex vertex) {
        Node n = ((TitanVertex)vertex).getRawElement();
        //Delete all edges
        Iterator<com.thinkaurelius.titan.core.Edge> iter = n.getEdgeIterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }
        n.delete();
        operation();
    }

    @Override
    public Iterable<Vertex> getVertices() {
        GraphTransaction tx = getAutoStartTx();
        return new TitanVertexSequence(this,tx.getAllNodes());
    }

    @Override
    public Edge addEdge(Object id, Vertex start, Vertex end, String s) {
        if (s.startsWith(INTERNAL_PREFIX)) throw new IllegalArgumentException("Edge labels cannot start with prefix " + INTERNAL_PREFIX);
        GraphTransaction tx = getAutoStartTx();
        if (!tx.containsEdgeType(s)) {
            GraphTransaction tx2 = db.startTransaction();
            tx2.createEdgeType().withName(s).withDirectionality(Directionality.Directed).category(EdgeCategory.Labeled).makeRelationshipType();
            tx2.commit();
        }
        Node startNode = ((TitanVertex)start).getRawElement();
        Node endNode = ((TitanVertex)end).getRawElement();
        Relationship r = tx.createRelationship(s,startNode,endNode);
        operation();
        return new TitanEdge(r,this);
    }

    @Override
    public Edge getEdge(Object o) {
        throw new UnsupportedOperationException("Edges cannot be retrieved by ID.");
    }

    @Override
    public void removeEdge(Edge edge) {
        ((TitanEdge)edge).getRawElement().delete();
        operation();
    }

    @Override
    public Iterable<Edge> getEdges() {
        GraphTransaction tx = getAutoStartTx();
        return new TitanEdgeSequence(this,tx.getAllRelationships());
    }

    @Override
    public void clear() {
        if (txs.get()!=null) stopTransaction(Conclusion.FAILURE);
        
        //TODO: remove everything from disk
//        GraphTransaction tx = db.startTransaction();
//        List<Node> nodes = new ArrayList<Node>();
//        for (Node node : tx.getAllNodes()) {
//            Iterator<com.thinkaurelius.titan.core.Edge> iter = node.getEdgeIterator();
//            while (iter.hasNext()) {
//                com.thinkaurelius.titan.core.Edge e = iter.next();
//                iter.remove();
//            }
//            nodes.add(node);
//        }
//        for (Node node : nodes) node.delete();
//        tx.commit();
    }



    /** ==================== Indexable Graph ================= **/

    PropertyType getPropertyType(String name) {
        GraphTransaction tx = getAutoStartTx();
        if (!tx.containsEdgeType(name))  {
            GraphTransaction tx2 = db.startTransaction();
            getPropertyType(tx2,name,false,Object.class);
            tx2.commit();
        }
        return tx.getPropertyType(name);
    }

    private PropertyType getPropertyType(GraphTransaction tx, String name, boolean index, Class<?> datatype) {
        if (name.startsWith(INTERNAL_PREFIX)) throw new IllegalArgumentException("Keys cannot start with prefix " + INTERNAL_PREFIX);
        if (tx.containsEdgeType(name)) {
            PropertyType t = tx.getPropertyType(name);
            if (index && t.getIndexType()==PropertyIndex.None) 
                throw new UnsupportedOperationException("Need to define particular index key before it is being used!");
            return t;
        } else {
            index = index || indexEveryProperty;
            EdgeTypeMaker etm = tx.createEdgeType();
            etm.withName(name).category(EdgeCategory.Simple).functional(true).dataType(datatype);

            if (index) etm.setIndex(PropertyIndex.Standard);

            PropertyType t= etm.makePropertyType();

            if (indexEveryProperty) t.createProperty(indexNameProperty,Index.VERTICES);
            t.createProperty(globalTagProperty,indexTag);
            return t;
        }
    }

    @Override
    public <T extends Element> Index<T> createManualIndex(String s, Class<T> tClass, Parameter... parameters) {
        throw new UnsupportedOperationException("Manual indexes are not supported by Titan");
    }

    @Override
    public <T extends Element> AutomaticIndex<T> createAutomaticIndex(String indexName, Class<T> indexClass, Set<String> autoIndexKeys, Parameter... parameters) {
        Preconditions.checkNotNull(indexName);
        Preconditions.checkArgument(autoIndexKeys!=null,"Global indexes are not supported. Use " + Index.VERTICES + " instead.");
        Preconditions.checkArgument(!autoIndexKeys.isEmpty(),"Need to specify index keys");
        Preconditions.checkArgument(indexClass.isAssignableFrom(Vertex.class),"Can only index vertices");


        Class<?> datatype = null;
        if (parameters.length>0) {
            Parameter<String,Class<?>> clazz = parameters[0];        
            datatype = clazz.getValue();
        } else datatype = Object.class;

        GraphTransaction tx = db.startTransaction();
        try {
            for (String key : autoIndexKeys) {
                PropertyType t= getPropertyType(tx,key,true,datatype);
                assert t.getIndexType()==PropertyIndex.Standard;
                t.createProperty(indexNameProperty,indexName);
            }
            tx.commit();
        } finally {
            if (tx.isOpen()) tx.abort();
        }

        return new TitanIndex(this,indexName,autoIndexKeys,TitanVertex.class);
    }

    @Override
    public <T extends Element> Index<T> getIndex(String indexName, Class<T> indexClass) {
        Set<String> keys = new HashSet<String>();
        GraphTransaction tx = db.startTransaction();
        for (Node node : tx.getNodesByAttribute(indexNameProperty, indexName)) {
            Preconditions.checkArgument(node instanceof PropertyType);
            keys.add(((PropertyType)node).getName());
        }
        tx.commit();
        if (keys.isEmpty()) throw new IllegalArgumentException("Unknown index: " + indexName);
        else return new TitanIndex(this,indexName,keys,TitanVertex.class);
    }

    @Override
    public Iterable<Index<? extends Element>> getIndices() {
        HashMultimap<String,String> properties = HashMultimap.create();
        GraphTransaction tx = db.startTransaction();
        for (Node node : tx.getNodesByAttribute(globalTagProperty, indexTag)) {
            Preconditions.checkArgument(node instanceof PropertyType);
            PropertyType pt = (PropertyType)node;

            for (Property p : pt.getProperties(indexNameProperty)) {
                properties.put(p.getString(),pt.getName());
            }
        }
        tx.commit();
        List<Index<? extends Element>> indexes = new ArrayList<Index<? extends Element>>();
        for (String indexName : properties.keySet()) {
            indexes.add(new TitanIndex(this,indexName,properties.get(indexName),TitanVertex.class));
        }
        return indexes;
    }

    @Override
    public void dropIndex(String s) {
        GraphTransaction tx = db.startTransaction();
        if (s.equals(Index.VERTICES)) {
            indexEveryProperty=false;
            PropertyType element = tx.getPropertyType(indexNameProperty);
            Iterator<Property> iter = element.getPropertyIterator(indexEverythingProperty);
            while(iter.hasNext()) {
                iter.next();
                iter.remove();
            }
            element.createProperty(indexEverythingProperty,false);
        }
        for (Node node : tx.getNodesByAttribute(indexNameProperty, Index.VERTICES)) {
            Preconditions.checkArgument(node instanceof PropertyType);
            Iterator<Property> iter = node.getPropertyIterator(indexNameProperty);
            while(iter.hasNext()) {
                if (iter.next().getString().equals(s)) {
                    iter.remove();
                }
            }
        }
        tx.commit();
    }

    Set<Node> indexRetrieval(String type, Object attribute) {
        return getAutoStartTx().getNodesByAttribute(type,attribute);
    }

    /** ==================== Transactional Graph ================= **/

    @Override
    public void setMaxBufferSize(int i) {
        Preconditions.checkArgument(i>=0);
        bufferSize=i;
    }

    @Override
    public int getMaxBufferSize() {
        return bufferSize;
    }

    @Override
    public int getCurrentBufferSize() {
        TransactionWrapper txw = txs.get();
        if (txw==null) return 0;
        else return txw.getCurrentBufferSize();
    }

    void operation() {
        TransactionWrapper txw = txs.get();
        if (txw==null) throw new IllegalStateException("A transaction has not yet been started.");
        txw.operation();
    }


    @Override
    public void stopTransaction(Conclusion conclusion) {
        if (txs.get()==null) return;
        GraphTransaction tx = txs.get().getTransaction();
        if (tx==null || tx.isClosed())
            throw new IllegalStateException("Inconsistent transactional state.");
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
        txs.set(new TransactionWrapper(tx,bufferSize));
        openTx.put(tx,Boolean.TRUE);
        return tx;
    }

    @Override
    public void startTransaction() {
        //if (txs.get()!=null) throw new IllegalStateException("Nested transactions are not supported!");
        getAutoStartTx();
    }
    
    private GraphTransaction getAutoStartTx() {
        GraphTransaction tx = null;
        if (txs.get()==null) {
            tx=internalStartTransaction();
        } else tx=txs.get().getTransaction();
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
