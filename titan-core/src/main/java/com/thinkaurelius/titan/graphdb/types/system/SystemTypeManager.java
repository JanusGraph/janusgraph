package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

public abstract class SystemTypeManager {

    public static final String systemETprefix = "#System#";

    public static final int SYSTEM_RELATIONTYPE_OFFSET = 8;

    private volatile static Map<Long, SystemRelationType> SYSTEM_TYPES_BY_ID;
    private volatile static Map<String, SystemRelationType> SYSTEM_TYPES_BY_NAME;

    static {
        synchronized (SystemTypeManager.class) {
            ImmutableMap.Builder<Long, SystemRelationType> idBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<String, SystemRelationType> nameBuilder = ImmutableMap.builder();
            for (SystemRelationType et : new SystemRelationType[]{SystemKey.TypeCategory,SystemKey.TypeDefinitionDesc,
                    SystemKey.TypeDefinitionProperty,SystemKey.TypeName,
                    SystemKey.VertexExists,SystemLabel.TypeDefinitionEdge}) {
                idBuilder.put(et.getID(), et);
                nameBuilder.put(et.getName(),et);
            }

            SYSTEM_TYPES_BY_ID = idBuilder.build();
            SYSTEM_TYPES_BY_NAME = nameBuilder.build();
        }
        assert SYSTEM_TYPES_BY_ID.size()==6;
        assert SYSTEM_TYPES_BY_NAME.size()==6;
    }

    public static boolean isSystemType(long id) {
        return SYSTEM_TYPES_BY_ID.containsKey(id);
    }

    public static SystemRelationType getSystemType(long id) {
        return SYSTEM_TYPES_BY_ID.get(id);
    }

    public static SystemRelationType getSystemType(String name) {
        return SYSTEM_TYPES_BY_NAME.get(name);
    }

    public static boolean isSystemType(String name) {
        return SYSTEM_TYPES_BY_NAME.containsKey(name);
    }


    public static final Set<? extends SystemRelationType> prepersistedSystemTypes = ImmutableSet.of(
            SystemKey.TypeName,
            SystemKey.TypeDefinitionProperty,
            SystemKey.TypeCategory);

}
