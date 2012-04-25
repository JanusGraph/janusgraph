package com.thinkaurelius.titan.core;

import cern.colt.list.AbstractLongList;

import java.util.Iterator;

/**
 * List of {@link com.thinkaurelius.titan.core.Node}s.
 * 
 * Basic interface for a list of nodes which supports retrieving individuals nodes or iterating over all of them, but
 * does not support modification.
 *
 * Depending on how the query was executed that returned this NodeList, getting node ids might be significantly faster
 * than retrieving node objects.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * @since	0.2.4
 *
 */
public interface NodeList extends Iterable<Node> {


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
	 * Sorts this list according to node ids in increasing order.
     * If the list is already sorted, invoking this method incurs no cost.
	 *
	 * 
	 * @return True, if this list is sorted in increasing order of node ids, else false.
     * @throws UnsupportedOperationException If not all nodes in this list have an id
	 */
	public void sort();

    /**
     * Returns a list of ids of all nodes in this list of nodes in the same order of the original node list.
     *
     * @return A list of ids of all nodes in this list of nodes in the same order of the original node list.
     */
    public AbstractLongList getIDs();

    /**
     * Returns the id of the node at the specified position
     *
     * @param pos The position of the node in the list
     * @return The id of that node
     */
    public long getID(int pos);
	
}
