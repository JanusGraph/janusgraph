package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;

import java.util.Map;
import java.util.Set;

public abstract class SystemTypeManager {

    public static final String systemETprefix = "#System#";

    public static final int SYSTEM_TYPE_OFFSET = 8;

    private volatile static Map<Long, SystemType> systemEdgeTypes = null;

    public static boolean isSystemRelationType(long id) {
        //TODO: also check if its a typeid?
        return IDManager.getTypeCount(id) <= SYSTEM_TYPE_OFFSET;
    }


    public static SystemType getSystemRelationType(long id) {
        if (systemEdgeTypes == null) {
            //Initialize
            synchronized (SystemTypeManager.class) {
                if (systemEdgeTypes == null) {
                    ImmutableMap.Builder<Long, SystemType> builder = ImmutableMap.builder();
                    for (SystemType et : SystemKey.KEY_MAP.values()) {
                        builder.put(Long.valueOf(et.getID()), et);
                    }
//                    for (SystemType et : SystemLabel.values()) {
//                        builder.put(Long.valueOf(et.getID()), et);
//                    }
                    systemEdgeTypes = builder.build();
                }
            }
        }
        SystemType et = systemEdgeTypes.get(Long.valueOf(id));
        if (et == null) throw new IllegalArgumentException("System edge type is unknown with ID:" + id);
        else return et;
    }


    public static final Set<? extends SystemType> prepersistedSystemTypes = ImmutableSet.of(
            SystemKey.TypeName,
            SystemKey.TypeDefinition,
            SystemKey.TypeClass);

}
