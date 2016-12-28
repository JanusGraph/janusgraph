package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import com.thinkaurelius.titan.graphdb.internal.Token;
import com.thinkaurelius.titan.graphdb.types.TypeUtil;

import java.util.Map;
import java.util.Set;

public abstract class SystemTypeManager {

    private volatile static Map<Long, SystemRelationType> SYSTEM_TYPES_BY_ID;
    private volatile static Map<String, SystemRelationType> SYSTEM_TYPES_BY_NAME;
    private static final Set<String> ADDITIONAL_RESERVED_NAMES;
    private static final char[] RESERVED_CHARS = {'{', '}', '"', Token.SEPARATOR_CHAR};

    static {
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


            ADDITIONAL_RESERVED_NAMES = ImmutableSet.of(
                "key", "vertex", "edge", "element", "property", "label");
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

    public static boolean isNotSystemName(TitanSchemaCategory category, String name) {
        TypeUtil.checkTypeName(category, name);
        if (SystemTypeManager.isSystemType(name.toLowerCase()) || Token.isSystemName(name))
            throw new IllegalArgumentException("Name cannot be in protected namespace: "+name);
        for (char c : RESERVED_CHARS)
            Preconditions.checkArgument(name.indexOf(c) < 0, "Name contains reserved character %s: %s", c, name);
        return true;
    }

    public static boolean isSystemType(String name) {
        return SYSTEM_TYPES_BY_NAME.containsKey(name) || ADDITIONAL_RESERVED_NAMES.contains(name);
    }


}
