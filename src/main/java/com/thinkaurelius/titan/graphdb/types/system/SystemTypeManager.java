package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.types.group.StandardTypeGroup;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class SystemTypeManager {
	
	public static final String systemETprefix = "#System#";
		
	public static final TypeGroup SYSTEM_TYPE_GROUP = new StandardTypeGroup((short)0,systemETprefix);
	
	
	private static Map<Long,SystemType> systemEdgeTypes = null;
	
	public static SystemType getSystemEdgeType(long id) {
		if (systemEdgeTypes==null) {
			//Initialize
			systemEdgeTypes=new HashMap<Long,SystemType>();
			for (SystemType et : SystemKey.values()) {
				assert !systemEdgeTypes.containsKey(et.getID());
				systemEdgeTypes.put(Long.valueOf(et.getID()), et);
			}
			for (SystemType et : SystemLabel.values()) {
				assert !systemEdgeTypes.containsKey(et.getID());
				systemEdgeTypes.put(Long.valueOf(et.getID()), et);
			}
		}
		SystemType et = systemEdgeTypes.get(Long.valueOf(id));
		if (et==null) throw new IllegalArgumentException("System edge type is unknown with ID:" + id);
		else return et;
	}


    public static final Set<? extends SystemType> prepersistedSystemTypes = ImmutableSet.of(SystemKey.TypeName,
                                                            SystemKey.PropertyTypeDefinition,
                                                            SystemKey.RelationshipTypeDefinition,
                                                            SystemKey.TypeClass);
    
}
