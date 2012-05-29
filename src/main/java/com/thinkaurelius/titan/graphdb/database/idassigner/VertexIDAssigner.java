package com.thinkaurelius.titan.graphdb.database.idassigner;

import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;

public interface VertexIDAssigner {

	public long getNewID(InternalTitanVertex node);

    public long getNewID(IDManager.IDType type, long groupid);
    
    public IDManager getIDManager();

    public void close();
    
}
