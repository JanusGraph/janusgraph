package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

public abstract class SystemTypeManager {

    public static final String systemETprefix = "#System#";

    public static final int SYSTEM_TYPE_OFFSET = 8;

    private volatile static Map<Long, SystemType> SYSTEM_EDGE_TYPES;

    static {
        synchronized (SystemTypeManager.class) {
            ImmutableMap.Builder<Long, SystemType> builder = ImmutableMap.builder();
            for (SystemType et : SystemKey.KEY_MAP.values()) {
                builder.put(et.getID(), et);
            }

            SYSTEM_EDGE_TYPES = builder.build();
        }
    }

    public static boolean isSystemRelationType(long id) {
        return SYSTEM_EDGE_TYPES.containsKey(id);
    }


    public static SystemType getSystemRelationType(long id) {
        SystemType type = SYSTEM_EDGE_TYPES.get(id);
        if (type == null)
            throw new IllegalArgumentException("System edge type is unknown with ID:" + id);

        return type;
    }


    public static final Set<? extends SystemType> prepersistedSystemTypes = ImmutableSet.of(
            SystemKey.TypeName,
            SystemKey.TypeDefinition,
            SystemKey.TypeClass);

}
