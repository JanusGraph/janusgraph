package com.thinkaurelius.titan.graphdb.loadingstatus;

import com.thinkaurelius.titan.graphdb.query.InternalTitanQuery;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.util.datastructures.BitMap;

public class DirectionTypeEncoder {

	
	public static final boolean hasAllCovered(byte code, InternalTitanQuery query) {
		for (EdgeDirection dir : EdgeDirection.values()) {
			if (!query.hasDirectionCondition() || query.isAllowedDirection(dir)) {
				if (dir==EdgeDirection.OUT && query.queryProperties()) {
					if (!BitMap.readBitb(code, dir.getID()*2+1)) return false;
				}
				if (query.queryRelationships()){
					if (!BitMap.readBitb(code, dir.getID()*2)) return false;
				}
			}
		}
		return true;
	}
	
	public static final byte loaded(byte code, InternalTitanQuery query) {
		for (EdgeDirection dir : EdgeDirection.values()) {
			if (!query.hasDirectionCondition() || query.isAllowedDirection(dir)) {
				if (dir==EdgeDirection.OUT && query.queryProperties()) {
					code = BitMap.setBitb(code, dir.getID()*2+1);
				}
				if (query.queryRelationships()){
					code = BitMap.setBitb(code, dir.getID() * 2);
				}
			}
		}
		return code;
	}
	
	
}
