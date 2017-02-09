// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.types.system;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;
import org.janusgraph.graphdb.internal.Token;
import org.janusgraph.graphdb.types.TypeUtil;

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
                    ImplicitKey.ID, ImplicitKey.JANUSGRAPHID, ImplicitKey.LABEL,
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

    public static boolean isNotSystemName(JanusGraphSchemaCategory category, String name) {
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
