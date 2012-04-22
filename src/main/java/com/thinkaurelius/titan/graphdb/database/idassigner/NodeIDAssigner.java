package com.thinkaurelius.titan.graphdb.database.idassigner;

import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

public interface NodeIDAssigner {

	public long getNewID(InternalNode node);

    public long getNewID(IDManager.IDType type, long groupid);
    
    public IDManager getIDManager();

    public void close();
    
}
