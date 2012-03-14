package com.thinkaurelius.titan.core;

import cern.colt.list.AbstractLongList;

import java.util.Iterator;

/**
 * List of {@link com.thinkaurelius.titan.core.Node}s.
 * 
 * Basic interface for a list of nodes which supports retrieving individuals nodes or iterating over all of them, but
 * does not support modification.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * @since	0.2.4
 *
 */
public interface NodeList extends Iterable<Node> {

	/**
	 * This NodeIDList is returned by {@link com.thinkaurelius.titan.core.NeighborhoodQuery#getNeighborhoodIDs()} when the
	 * limit set via {@link com.thinkaurelius.titan.core.NeighborhoodQuery#setRetrievalLimit(long, boolean)} has been
	 * exceeded while retrieving the neighborhood.
	 */
	public static final NodeIDList LimitExceeded = new NodeIDList() {

		@Override
		public long getID(int pos) {
			throw new IllegalStateException("Not supported on LimitExceeded placehold list.");
		}

		@Override
		public AbstractLongList getIDs() {
			throw new IllegalStateException("Not supported on LimitExceeded placehold list.");
		}

		@Override
		public Node get(int pos) {
			throw new IllegalStateException("Not supported on LimitExceeded placehold list.");
		}

		@Override
		public boolean isSorted() {
			throw new IllegalStateException("Not supported on LimitExceeded placehold list.");
		}

		@Override
		public int size() {
			throw new IllegalStateException("Not supported on LimitExceeded placehold list.");
		}

		@Override
		public Iterator<Node> iterator() {
			throw new IllegalStateException("Not supported on LimitExceeded placehold list.");
		}
		
	};
	
	/**
	 * Returns the number of nodes in this list.
	 * 
	 * @return Number of nodes in the list.
	 */
	public int size();
	
	/**
	 * Returns the node at a given position in the list.
	 * 
	 * @param pos Position for which to retrieve the node.
	 * @return Node at the given position
	 */
	public Node get(int pos);
	
	/**
	 * Checks whether this list of nodes is sorted according to node ids in increasing order.
	 * Note that this is necessarily false if not all nodes in this list have ids.
	 * 
	 * @return True, if this list is sorted in increasing order of node ids, else false.
	 */
	public boolean isSorted();
	
}
