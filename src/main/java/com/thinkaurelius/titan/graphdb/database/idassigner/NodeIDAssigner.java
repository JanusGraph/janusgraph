package com.thinkaurelius.titan.graphdb.database.idassigner;

import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

public interface NodeIDAssigner {

	public long getNewNodeID(InternalNode node);

    public long getNewEdgeID(InternalEdge edge);

    public IDManager getIDManager();

    public void close();
    
}
