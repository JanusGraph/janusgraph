package com.thinkaurelius.titan.graphdb.vertices.factory;

import com.thinkaurelius.titan.graphdb.adjacencylist.InitialAdjListFactory;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.DualNode;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;
import com.thinkaurelius.titan.graphdb.vertices.PersistDualNode;

public enum StandardNodeFactories implements NodeFactory {

	DefaultPersisted {

		@Override
		public InternalNode createExisting(GraphTx tx, long id) {
			return new PersistDualNode(tx,InitialAdjListFactory.BasicFactory,id);
		}


		@Override
		public InternalNode createNew(GraphTx tx) {
			InternalNode n = new PersistDualNode(tx,InitialAdjListFactory.BasicFactory);
            tx.registerNewEntity(n);
            return n;
		}
	
		
	},
	
	DefaultInMemory {
	
		@Override
		public InternalNode createExisting(GraphTx tx, long id) {
			throw new UnsupportedOperationException("Cannot create existing vertices for in-memory transaction!");
		}


		@Override
		public InternalNode createNew(GraphTx tx) {
			InternalNode n = new DualNode(tx,InitialAdjListFactory.BasicFactory);
            tx.registerNewEntity(n);
            return n;
		}
		
	};
	
	

	
	
}
