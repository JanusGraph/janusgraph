package com.thinkaurelius.titan.core;

import cern.colt.list.AbstractLongList;

/**
 * List of {@link com.thinkaurelius.titan.core.Node} IDs.
 * 
 * Basic interface for a list of node ids which supports retrieving individuals node ids or getting all
 * nodes ids.
 * This list does not support modification.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * @since	0.2.4
 *
 */
public interface NodeIDList extends NodeList {

	public AbstractLongList getIDs();
	
	public long getID(int pos);
	
}
