package com.thinkaurelius.titan.traversal;

import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.Relationship;

/**
 * Allows definition of custom evaluator called upon during traversal.
 * 
 * To analyze and or manipulate the subgraph visited during a traversal, the user can provide a custom implementation of TraversalEvaluator
 * which gets called for each node and relationship visited during the traversal.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 */
public interface TraversalEvaluator {

	/**
	 * Called exactly once for each node encountered during the traversal.
	 * The return value indicates whether this node should be included in the traversal.
	 * 
	 * By returning false, this method can effectively end a traversal conditionally (i.e. based on some property of the nodes).
	 * In such cases, one can get around setting a traversal depth limit if it is ensured that the traversal is terminated by means of this function.
	 * 
	 * @param node Node encountered during the traversal
	 * @return true, if the node should be visited by the traversal (and its neighborhood explored) or false, if it should be ignored and not visited.
	 */
	public boolean nextNode(Node node);
	
	/**
	 * Called at least once (but possibly more often) for each edge encountered in the traversal.
	 * 
	 * It is important to note that it is not guaranteed that this method will only be called once for
	 * each relationship.
	 * 
	 * @param edge Relationship encountered during the traversal
	 */
	public void nextRelationship(Relationship edge);
	
}
