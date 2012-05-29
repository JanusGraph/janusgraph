package com.thinkaurelius.titan.graphdb.types.manager;

import com.thinkaurelius.titan.core.TitanType;

public class TypeManagerUtil {

	public static final String[] convertSignature(TitanType[] sig) {
		String[] res = new String[sig.length];
		for (int i=0;i<sig.length;i++) {
			res[i]=sig[i].getName();
		}
		return res;
	}
	
}
