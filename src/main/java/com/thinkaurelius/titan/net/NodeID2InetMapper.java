package com.thinkaurelius.titan.net;

import java.net.InetAddress;

public interface NodeID2InetMapper {

	public InetAddress[] getInetAddress(long nodeId);
	
	public boolean isNodeLocal(long nodeId);
}
