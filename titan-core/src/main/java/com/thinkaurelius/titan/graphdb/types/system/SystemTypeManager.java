package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.collect.ImmutableMap;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.Map;

public abstract class SystemTypeManager {

    public static final String systemETprefix = "^internal$";

    private volatile static Map<Long, SystemRelationType> SYSTEM_TYPES_BY_ID;
    private volatile static Map<String, SystemRelationType> SYSTEM_TYPES_BY_NAME;

    static {
        assert Graph.System.isSystem(systemETprefix);
        synchronized (SystemTypeManager.class) {
            ImmutableMap.Builder<Long, SystemRelationType> idBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<String, SystemRelationType> nameBuilder = ImmutableMap.builder();
            for (SystemRelationType et : new SystemRelationType[]{BaseKey.SchemaCategory, BaseKey.SchemaDefinitionDesc,
                    BaseKey.SchemaDefinitionProperty, BaseKey.SchemaName, BaseKey.SchemaUpdateTime,
                    BaseKey.VertexExists,
                    BaseLabel.VertexLabelEdge, BaseLabel.SchemaDefinitionEdge,
                    ImplicitKey.ID, ImplicitKey.TITANID, ImplicitKey.LABEL,
                    ImplicitKey.KEY, ImplicitKey.VALUE, ImplicitKey.ADJACENT_ID,
                    ImplicitKey.TIMESTAMP, ImplicitKey.TTL, ImplicitKey.VISIBILITY
                }) {
                idBuilder.put(et.longId(), et);
                nameBuilder.put(et.name(),et);
            }

            SYSTEM_TYPES_BY_ID = idBuilder.build();
            SYSTEM_TYPES_BY_NAME = nameBuilder.build();
        }
        assert SYSTEM_TYPES_BY_ID.size()==17;
        assert SYSTEM_TYPES_BY_NAME.size()==17;
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


}
