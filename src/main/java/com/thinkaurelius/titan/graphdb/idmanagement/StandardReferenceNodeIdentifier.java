package com.thinkaurelius.titan.graphdb.idmanagement;

import com.thinkaurelius.titan.net.NodeID2InetMapper;

public class StandardReferenceNodeIdentifier implements ReferenceNodeIdentifier {

	private final NodeID2InetMapper mapper;
	
	public StandardReferenceNodeIdentifier(NodeID2InetMapper mapper) {
		this.mapper = mapper;
	}

	@Override
	public boolean isReferenceNodeID(long vid) {
		return !mapper.isNodeLocal(vid);
	}
}
