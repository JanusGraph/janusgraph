package com.thinkaurelius.titan.decorators;

import com.thinkaurelius.titan.core.Node;


/**
 * Allows custom weightings of nodes in a graph database.
 * 
 * Some graph algorithms require users to provide or will return a weighting of nodes as an implementation of this interface.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 */
public interface NodeWeighter {

	/**
	 * Implementation of {@link com.thinkaurelius.titan.decorators.NodeWeighter} which returns 1 for each node.
	 */
	public final static NodeWeighter ConstantOne = new NodeWeighter() {
		@Override
		public final double getWeight(Node n) {
			return 1;
		}
	};
	
	/**
	 * Returns the weight of the given node.
	 * 
	 * @param n Node to weigh
	 * @return weight of the node
	 */
	public double getWeight(Node n);
	
}
