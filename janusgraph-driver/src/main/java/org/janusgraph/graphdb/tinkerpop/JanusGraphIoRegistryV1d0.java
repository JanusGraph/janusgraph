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

package org.janusgraph.graphdb.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.tinkerpop.io.JanusGraphP;
import org.janusgraph.graphdb.tinkerpop.io.graphson.JanusGraphSONModuleV1d0;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Stephen Mallette (https://stephen.genoprime.com)
 */
public class JanusGraphIoRegistryV1d0 extends AbstractIoRegistry {

    /**
     * Fully qualified names of the JanusGraph types which GraphSON 1.0 embeds as {@code @class} type ids when
     * {@link org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo#PARTIAL_TYPES} is used.
     * <p>
     * Since TinkerPop 3.8.2 GraphSON 1.0 only deserializes type ids which are explicitly allowed, so these names have to
     * be registered via {@link GraphSONMapper.Builder#addAllowedTypeIdName(String...)} (see
     * {@link #allowGraphSONTypeIds(GraphSONMapper.Builder)}) or the {@code allowedTypeIdNames} config of
     * {@code GraphSONMessageSerializerV1} next to this registry whenever JanusGraph types are exchanged with embedded
     * types. {@code JanusGraph#io(Io.Builder)} does this automatically.
     */
    public static final List<String> GRAPHSON_ALLOWED_TYPE_ID_NAMES = Collections.unmodifiableList(Arrays.asList(
        RelationIdentifier.class.getName(),
        Geoshape.class.getName(),
        JanusGraphP.class.getName()
    ));

    /**
     * Allows the JanusGraph GraphSON 1.0 type ids ({@link #GRAPHSON_ALLOWED_TYPE_ID_NAMES}) on the given mapper builder.
     *
     * @return the given builder
     */
    public static GraphSONMapper.Builder allowGraphSONTypeIds(final GraphSONMapper.Builder builder) {
        return builder.addAllowedTypeIdName(GRAPHSON_ALLOWED_TYPE_ID_NAMES.toArray(new String[0]));
    }

    private static final JanusGraphIoRegistryV1d0 INSTANCE = new JanusGraphIoRegistryV1d0();

    private JanusGraphIoRegistryV1d0() {
        register(GraphSONIo.class, null, JanusGraphSONModuleV1d0.getInstance());
        register(GryoIo.class, RelationIdentifier.class, null);
        register(GryoIo.class, Geoshape.class, new Geoshape.GeoShapeGryoSerializer());
        register(GryoIo.class, JanusGraphP.class, new JanusGraphPSerializer());
    }

    public static JanusGraphIoRegistryV1d0 instance() {
        return INSTANCE;
    }
}
