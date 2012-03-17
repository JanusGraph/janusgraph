package com.thinkaurelius.titan.graphdb.database.idassigner.local;


import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.core.RelationshipType;
import com.thinkaurelius.titan.graphdb.database.idassigner.NodeIDAssigner;
import com.thinkaurelius.titan.graphdb.database.serialize.ObjectDiskStorage;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.idmanagement.ReferenceNodeIdentifier;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

import java.util.Random;


public class SimpleNodeIDAssigner implements NodeIDAssigner {
	
	private static final String nodeCounterFile = "nodeIDcounter";
	private static final String edgeCounterFile = "edgeIDcounter";
	private static final String edgetypeCounterFile = "edgetypeIDcounter";

	
	private final LocalID node;
	private final LocalID edge;
	private final LocalID edgeType;

	private final IDManager idManager;
	private final long maxPartitionID;
	
	private final Random randomSource;
	
	
	public SimpleNodeIDAssigner(IDManager idManager, int inserterID, int maxNoInserter,
                                ObjectDiskStorage objectStore) {
		this.idManager = idManager;
		int max = maxNoInserter;
		int inserter = inserterID;
		maxPartitionID = idManager.getMaxPartitionID();

		node = new LocalID(idManager.getMaxNodeID(),max,inserter,objectStore,nodeCounterFile);
		edge = new LocalID(idManager.getMaxNodeID(),max,inserter,objectStore,edgeCounterFile);
		edgeType = new LocalID(idManager.getMaxNodeID(),max,inserter,objectStore,edgetypeCounterFile);
		randomSource = new Random();
	}

    @Override
    public long getNewID(InternalNode node) {
        assert !node.hasID();
        if (node instanceof InternalEdge) {
            return nextEdgeID();
        } else if (node instanceof PropertyType) {
            return nextPropertyTypeID(((EdgeType)node).getGroup().getID());
        } else if (node instanceof RelationshipType) {
            return nextRelationshipTypeID(((EdgeType)node).getGroup().getID());
        } else {
            return nextNodeID();
        }
    }
    
    @Override
    public long getNewID(IDManager.IDType type, long groupid) {
        switch (type) {
            case Edge : return nextEdgeID();
            case PropertyType: return nextPropertyTypeID(groupid);
            case RelationshipType: return nextRelationshipTypeID(groupid);
            case Node: return nextNodeID();
            default: throw new IllegalArgumentException("ID type not supported: " + type);
        }
    }

    @Override
    public IDManager getIDManager() {
        return idManager;
    }

    @Override
	public synchronized void close() {
		node.close();
		edge.close();
		edgeType.close();
	}

    private final long getPartitionID() {
        return randomSource.nextInt((int)maxPartitionID)+1;
    }

	private long nextEdgeID() {
		return idManager.getEdgeID(edge.nextID());
	}

	private long nextNodeID() {
		return idManager.getNodeID(node.nextID(), getPartitionID());
	}

	public long nextPropertyTypeID(long groupid) {
		return idManager.getPropertyTypeID(edgeType.nextID(), groupid, getPartitionID());
	}

	private long nextRelationshipTypeID(long groupid) {
		return idManager.getRelationshipTypeID(edgeType.nextID(), groupid, getPartitionID());
	}


	
	
	
}
