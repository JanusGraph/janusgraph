package com.thinkaurelius.titan.graphdb.transaction;

import cern.colt.list.AbstractLongList;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.GraphTransactionConfig;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.exceptions.GraphStorageException;
import com.thinkaurelius.titan.graphdb.database.GraphDB;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.edges.factory.StandardPersistedEdgeFactory;
import com.thinkaurelius.titan.graphdb.edgetypes.manager.EdgeTypeManager;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;
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
	

	private final TransactionHandle txHandle;
		
	private Set<InternalEdge> deletedEdges;
	private List<InternalEdge> addedEdges;
	

	
	public StandardPersistGraphTx(GraphDB g, EdgeTypeManager etManage, GraphTransactionConfig config,
						TransactionHandle tx) {
		super(g,StandardNodeFactories.DefaultPersisted,new StandardPersistedEdgeFactory(),
				etManage,config);
		Preconditions.checkNotNull(g);
		txHandle = tx;

		if (config.isReadOnly()) {
			deletedEdges = null;
			addedEdges = null;
		} else {
			deletedEdges = Collections.newSetFromMap(new ConcurrentHashMap<InternalEdge,Boolean>(10,0.75f,1));
			addedEdges = Collections.synchronizedList(new ArrayList<InternalEdge>());
		}
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

	@Override
	public boolean containsNode(long id) {
		if (super.containsNode(id)) return true;
		else return graphdb.containsNodeID(id, this);
	}

	@Override
	public Iterable<? extends Node> getAllNodes() {
		//return Iterables.concat(super.getAllNodes(),this.getAllExistingNodes());
        return super.getAllNodes();
	}


	@Override
	public void deletedEdge(InternalEdge edge) {
		super.deletedEdge(edge);
		if (edge.isNew()) return;
		if (!edge.isInline()) {
			//Only store those deleted edges that matter, i.e. those that we need to erase from memory on their own		
			boolean success = deletedEdges.add(edge);
			assert success;
		}
	}
	

	@Override
	public void addedEdge(InternalEdge edge) {
		super.addedEdge(edge);
		if (!edge.isInline()) {
			//Only store those added edges that matter, i.e. those that we need to erase from memory on their own
			addedEdges.add(edge);
		}
		
	}
	
	@Override
	public void loadedEdge(InternalEdge edge) {
		super.loadedEdge(edge);
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
			long[] ids = graphdb.indexRetrieval(key, type, this);
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
	public long[] getNodeIDsByAttributeFromDisk(PropertyType type, Object attribute) {
		Preconditions.checkArgument(type.hasIndex(),"Can only retrieve nodes for indexed property types.");
		if (!type.isNew()) {
			long[] ids = graphdb.indexRetrieval(attribute, type, this);
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
		addedEdges=null;
		deletedEdges=null;
	}


    @Override
    public synchronized void rollingCommit() {
        super.rollingCommit();
        if (!getTxConfiguration().isReadOnly()) {
            try {
                graphdb.save(addedEdges, deletedEdges, this);
            } catch (GraphStorageException e) {
                abort();
                throw e;
            }
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
            try {
                graphdb.save(added, deleted, this);
            } catch (GraphStorageException e) {
                abort();
                throw e;
            }
        }

        clear();
        txHandle.commit();
		super.commit();
	}

	@Override
	public synchronized void abort() {
		clear();
		txHandle.abort();
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
