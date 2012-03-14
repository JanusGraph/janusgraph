package com.thinkaurelius.titan.graphdb.edgetypes.manager;

import com.thinkaurelius.titan.core.EdgeType;

public class EdgeTypeManagerUtil {

	public static final String[] convertSignature(EdgeType[] sig) {
		String[] res = new String[sig.length];
		for (int i=0;i<sig.length;i++) {
			res[i]=sig[i].getName();
		}
		return res;
	}
	
}
