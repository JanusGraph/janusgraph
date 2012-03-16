package com.thinkaurelius.titan.graphdb.transaction;

import cern.colt.list.AbstractLongList;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.core.GraphTransactionConfig;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.core.attribute.Interval;
import com.thinkaurelius.titan.core.attribute.PointInterval;
import com.thinkaurelius.titan.core.query.QueryType;
import com.thinkaurelius.titan.core.query.ResultCollector;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.exceptions.InvalidNodeException;
import com.thinkaurelius.titan.graphdb.database.GraphDB;
import com.thinkaurelius.titan.graphdb.database.LockManager;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.edges.factory.StandardPersistedEdgeFactory;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalEdgeType;
import com.thinkaurelius.titan.graphdb.edgetypes.manager.EdgeTypeManager;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.sendquery.QuerySender;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;
import com.thinkaurelius.titan.graphdb.vertices.StandardReferenceNode;
import com.thinkaurelius.titan.graphdb.vertices.factory.StandardNodeFactories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StandardPersistGraphTx extends AbstractGraphTx {

	private static final Logger log = LoggerFactory.getLogger(StandardPersistGraphTx.class);
	
	private final GraphDB graphdb;
	private final TransactionHandle txHandle;
	private final LockManager lockManager;
	
	private final QuerySender querySender;
		
	private Set<InternalEdge> deletedEdges;
	private List<InternalEdge> addedEdges;
	
	private NodeCache nodeCache;
	
	public StandardPersistGraphTx(GraphDB g, LockManager locks, 
						EdgeTypeManager etManage, GraphTransactionConfig config,
						QuerySender sender, TransactionHandle tx) {
		super(StandardNodeFactories.DefaultPersisted,new StandardPersistedEdgeFactory(),
				etManage,config,false);
		Preconditions.checkNotNull(g);
		graphdb = g;
		lockManager = locks;
		querySender = sender;
		txHandle = tx;
		
		nodeCache = new StandardNodeCache();
		if (config.isReadOnly()) {
			deletedEdges = null;
			addedEdges = null;
		} else {
			deletedEdges = Collections.newSetFromMap(new ConcurrentHashMap<InternalEdge,Boolean>(10,0.75f,1));
			addedEdges = Collections.synchronizedList(new ArrayList<InternalEdge>());
		}
        Preconditions.checkNotNull(addedEdges);
	}


	/* ---------------------------------------------------------------
	 * Node and Edge creation
	 * ---------------------------------------------------------------
	 */
	
	@Override
	public boolean isDeletedEdge(InternalEdge e) {
		if (e.isDeleted()) return true;
		else return deletedEdges.contains(e);
	}
	

	public boolean isReferenceNode(long nodeID) {
		return graphdb.getIDInspector().isReferenceNodeID(nodeID);
	}
		
	@Override
	public boolean containsNode(long id) {
		if (nodeCache.contains(id)) return true;
		else return graphdb.containsNodeID(id, this);
	}


	@Override
	public Node getNode(long id) {
		if (getTxConfiguration().doVerifyNodeExistence() && 
				!containsNode(id)) throw new InvalidNodeException("Node does not exist!");
		return getExistingNode(id);
	}


	@Override
	public InternalNode getExistingNode(long id) {
		return getExisting(id);
	}

	private InternalNode getExisting(long id) {
		synchronized(nodeCache) {
		InternalNode node = nodeCache.get(id);
		if (node==null) {
			IDInspector idspec = graphdb.getIDInspector();
	
			if (idspec.isEdgeTypeID(id)) {
				node = etManager.getEdgeType(id, this);
			} else if (idspec.isReferenceNodeID(id)) {
				return new StandardReferenceNode(this, id);
			} else if (idspec.isNodeID(id)) {
				node = nodeFactory.createExisting(this,id);
			} else throw new IllegalArgumentException("ID could not be recognized!");
			nodeCache.add(node, id);
		}
		return node;
		}

	}

	@Override
	public boolean containsEdgeType(String name) {
		boolean contains = super.containsEdgeType(name);
		if (!contains) {
			contains = etManager.containsEdgeType(name, this);
		}
		return contains;
	}
	
	@Override
	public EdgeType getEdgeType(String name) {
		//First, check local index
		EdgeType et = super.getEdgeType(name);
		if (et==null) {
 			//Second, check EdgeTypeManager
			InternalEdgeType eti = etManager.getEdgeType(name, this);
			if (eti!=null)
				nodeCache.add(eti, eti.getID());
			et=eti;
		}
		return et;
	}
	

	private Iterable<? extends Node> getAllExistingNodes() {
		throw new UnsupportedOperationException("Retrieving all nodes is not supported for this transaction. Use the traversal framework instead.");
	}

	@Override
	public Iterable<? extends Node> getAllNodes() {
		return Iterables.concat(super.getAllNodes(),this.getAllExistingNodes());
	}


	@Override
	public void deletedEdge(InternalEdge edge) {
		super.deletedEdge(edge);
		if (edge.isNew()) return;
		if (!edge.isInline()) {
			//Only store those deleted edges that matter, i.e. those that we need to erase from memory on their own
			lockManager.deleteEdge(edge);		
			boolean success = deletedEdges.add(edge);
			assert success;
		}
	}
	

	@Override
	public void addedEdge(InternalEdge edge) {
		super.addedEdge(edge);
		if (!edge.isInline()) {
			//Only store those added edges that matter, i.e. those that we need to erase from memory on their own
			lockManager.createEdge(edge);
			addedEdges.add(edge);
		}
		
	}
	
	@Override
	public void loadedEdge(InternalEdge edge) {
		super.loadedEdge(edge);
	}
	
	@Override
	public<T,U> void sendQuery(long nodeid, T queryLoad, 
			Class<? extends QueryType<T,U>> queryType, 
					ResultCollector<U> resultCollector) {
		assert isReferenceNode(nodeid);
		querySender.sendQuery(nodeid, queryLoad, queryType, resultCollector);		
	}
	
	@Override
	public void forwardQuery(long nodeid, Object queryLoad) {
		assert isReferenceNode(nodeid);
		querySender.forwardQuery(nodeid, queryLoad);
	}

	
	/* ---------------------------------------------------------------
	 * Index Handling
	 * ---------------------------------------------------------------
	 */	
	

	@Override
	public Node getNodeByKey(PropertyType type, Object key) {
		Node node = super.getNodeByKey(type, key);
		if (node==null && !type.isNew()) { 
			//Look up
			long[] ids = graphdb.indexRetrieval(new PointInterval<Object>(key), type, this);
			if (ids.length==0) {
                //TODO Set NO-ENTRY
                return null;
            } else {
				assert ids.length==1;
				InternalNode n = getExistingNode(ids[0]);
				addProperty2Index(type, key, n);
				return n;
			}
		} else return node;
	}
	
	@Override
	public long[] getNodeIDsByAttributeFromDisk(PropertyType type, Interval<?> interval) {
		Preconditions.checkArgument(type.getIndexType().hasIndex(),"Can only retrieve nodes for indexed property types.");
		if (!type.isNew()) {
			long[] ids = graphdb.indexRetrieval(interval, type, this);
			return ids;
		} else return new long[0];
	}
	
	/* ---------------------------------------------------------------
	 * Property / Edge Loading
	 * ---------------------------------------------------------------
	 */	

	
	@Override
	public void loadEdges(InternalEdgeQuery query) {
		graphdb.loadEdges(query, this);
	}
	
	@Override
	public AbstractLongList getRawNeighborhood(InternalEdgeQuery query) {
		return graphdb.getRawNeighborhood(query, this);
	}
	
	private void clear() {
		nodeCache.close(); nodeCache=null;
		addedEdges=null;
		deletedEdges=null;
	}

	@Override
	public synchronized void flush() {
        if (!getTxConfiguration().isReadOnly()) {
            graphdb.flush(addedEdges, deletedEdges, this);
            //Update NodeCache
            addNodes2Cache();
        }
	}

    private void addNodes2Cache() {
        for (InternalEdge edge : addedEdges) {
            for (int a=0;a<edge.getArity();a++) {
                InternalNode n = edge.getNodeAt(a);
                if (!nodeCache.contains(n.getID())) nodeCache.add(n, n.getID());
            }
        }
    }

    @Override
    public synchronized void rollingCommit() {
        if (!getTxConfiguration().isReadOnly()) {
            flush();
            graphdb.save(addedEdges, deletedEdges, this);
            deletedEdges = Collections.newSetFromMap(new ConcurrentHashMap<InternalEdge,Boolean>(10,0.75f,1));
            addedEdges = Collections.synchronizedList(new ArrayList<InternalEdge>());
        }
    }
	
	@Override
	public synchronized void commit() {
        if (!getTxConfiguration().isReadOnly()) {
            List<InternalEdge> added=addedEdges;
            Set<InternalEdge> deleted=deletedEdges;
            addedEdges=null;
            deletedEdges=null;
            graphdb.save(added, deleted, this);
        }

        txHandle.commit();
		clear();
		querySender.commit();
		super.commit();
	}

	@Override
	public synchronized void abort() {
		clear();
		txHandle.abort();
		querySender.abort();
		super.abort();
	}
	
	@Override
	public TransactionHandle getTxHandle() {
		return txHandle;
	}

	@Override
	public boolean hasModifications() {
		return !getTxConfiguration().isReadOnly() && (!deletedEdges.isEmpty() || !addedEdges.isEmpty());
	}














	
}
