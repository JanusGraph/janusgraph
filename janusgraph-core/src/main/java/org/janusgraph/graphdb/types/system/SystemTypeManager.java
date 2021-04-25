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
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;
import org.janusgraph.graphdb.internal.Token;
import org.janusgraph.graphdb.types.TypeUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class SystemTypeManager {

    private static final Map<Long, SystemRelationType> SYSTEM_TYPES_BY_ID;
    private static final Map<String, SystemRelationType> SYSTEM_TYPES_BY_NAME;
    private static final Set<String> ADDITIONAL_RESERVED_NAMES;
    private static final char[] RESERVED_CHARS = {'{', '}', '"', Token.SEPARATOR_CHAR};

    static {
        synchronized (SystemTypeManager.class) {
            SystemRelationType[] systemRelationTypes = new SystemRelationType[]{BaseKey.SchemaCategory, BaseKey.SchemaDefinitionDesc,
                BaseKey.SchemaDefinitionProperty, BaseKey.SchemaName, BaseKey.SchemaUpdateTime,
                BaseKey.VertexExists,
                BaseLabel.VertexLabelEdge, BaseLabel.SchemaDefinitionEdge,
                ImplicitKey.ID, ImplicitKey.JANUSGRAPHID, ImplicitKey.LABEL,
                ImplicitKey.KEY, ImplicitKey.VALUE, ImplicitKey.ADJACENT_ID,
                ImplicitKey.TIMESTAMP, ImplicitKey.TTL, ImplicitKey.VISIBILITY
            };
            Map<Long, SystemRelationType> idBuilder = new HashMap<>(systemRelationTypes.length);
            Map<String, SystemRelationType> nameBuilder = new HashMap<>(systemRelationTypes.length);
            for (SystemRelationType et : systemRelationTypes) {
                idBuilder.put(et.longId(), et);
                nameBuilder.put(et.name(),et);
            }
            SYSTEM_TYPES_BY_ID = Collections.unmodifiableMap(idBuilder);
            SYSTEM_TYPES_BY_NAME = Collections.unmodifiableMap(nameBuilder);

            ADDITIONAL_RESERVED_NAMES = Collections.unmodifiableSet(
                Stream.of("key", "vertex", "edge", "element", "property", "label").collect(Collectors.toSet()));
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

    public static void throwIfSystemName(JanusGraphSchemaCategory category, String name) {
        TypeUtil.checkTypeName(category, name);
        if (SystemTypeManager.isSystemType(name.toLowerCase()) || Token.isSystemName(name))
            throw new IllegalArgumentException("Name cannot be in protected namespace: "+name);
        for (char c : RESERVED_CHARS)
            Preconditions.checkArgument(name.indexOf(c) < 0, "Name contains reserved character %s: %s", c, name);
    }

    public static boolean isSystemType(String name) {
        return SYSTEM_TYPES_BY_NAME.containsKey(name) || ADDITIONAL_RESERVED_NAMES.contains(name);
    }


}
