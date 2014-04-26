package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;

import java.util.Map;
import java.util.Set;

public abstract class SystemTypeManager {

    public static final String systemETprefix = "system%&%";

    private volatile static Map<Long, SystemType> SYSTEM_TYPES_BY_ID;
    private volatile static Map<String, SystemType> SYSTEM_TYPES_BY_NAME;

    static {
        synchronized (SystemTypeManager.class) {
            ImmutableMap.Builder<Long, SystemType> idBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<String, SystemType> nameBuilder = ImmutableMap.builder();
            for (SystemType et : new SystemType[]{BaseKey.TypeCategory, BaseKey.TypeDefinitionDesc,
                    BaseKey.TypeDefinitionProperty, BaseKey.TypeName,
                    BaseKey.VertexExists, BaseLabel.TypeDefinitionEdge,
                    ImplicitKey.ID, ImplicitKey.LABEL, ImplicitKey.TIMESTAMP, ImplicitKey.VISIBILITY
                }) {
                if (et.hasId()) idBuilder.put(et.getID(), et);
                nameBuilder.put(et.getName(),et);
            }

            SYSTEM_TYPES_BY_ID = idBuilder.build();
            SYSTEM_TYPES_BY_NAME = nameBuilder.build();
        }
        assert SYSTEM_TYPES_BY_ID.size()==8;
        assert SYSTEM_TYPES_BY_NAME.size()==10;
    }

    public static SystemType getSystemType(long id) {
        return SYSTEM_TYPES_BY_ID.get(id);
    }

    public static SystemType getSystemType(String name) {
        return SYSTEM_TYPES_BY_NAME.get(name);
    }

    public static boolean isSystemType(String name) {
        return SYSTEM_TYPES_BY_NAME.containsKey(name);
    }

}
