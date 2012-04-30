package com.thinkaurelius.titan.graphdb.edgetypes.system;

import com.thinkaurelius.titan.core.EdgeTypeGroup;
import com.thinkaurelius.titan.graphdb.edgetypes.group.StandardEdgeTypeGroup;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;

import java.util.HashMap;
import java.util.Map;

public abstract class SystemEdgeTypeManager {
	
	public static final String systemETprefix = "#System#";
		
	public static final EdgeTypeGroup systemETgroup = new StandardEdgeTypeGroup((short)0,systemETprefix);
	
	
	private static Map<Long,SystemEdgeType> systemEdgeTypes = null;
	
	public SystemEdgeType getSystemEdgeType(long id) {
		if (systemEdgeTypes==null) {
			//Initialize
			systemEdgeTypes=new HashMap<Long,SystemEdgeType>();
			for (SystemEdgeType et : SystemPropertyType.values()) {
				assert !systemEdgeTypes.containsKey(et.getID());
				systemEdgeTypes.put(Long.valueOf(et.getID()), et);
			}
			for (SystemEdgeType et : SystemRelationshipType.values()) {
				assert !systemEdgeTypes.containsKey(et.getID());
				systemEdgeTypes.put(Long.valueOf(et.getID()), et);
			}
		}
		SystemEdgeType et = systemEdgeTypes.get(Long.valueOf(id));
		if (et==null) throw new IllegalArgumentException("System edge type is unknown with ID:" + id);
		else return et;
	}

    //TODO: This needs to be adjusted to the changes in IDManager


	public final static long getInternalRelationshipID(long id) {
		return IDManager.IDType.RelationshipType.addPadding(id<< IDManager.maxGroupBits);
	}
	
	public final static long getInternalPropertyID(long id) {
		return IDManager.IDType.PropertyType.addPadding(id<< IDManager.maxGroupBits);
	}
}
