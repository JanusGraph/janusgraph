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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoSerializersV3d0;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.tinkerpop.io.JanusGraphP;
import org.janusgraph.graphdb.tinkerpop.io.binary.GeoshapeGraphBinarySerializer;
import org.janusgraph.graphdb.tinkerpop.io.binary.JanusGraphPBinarySerializer;
import org.janusgraph.graphdb.tinkerpop.io.binary.RelationIdentifierGraphBinarySerializer;
import org.janusgraph.graphdb.tinkerpop.io.graphson.JanusGraphSONModuleV2d0;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Stephen Mallette (https://stephen.genoprime.com)
 */
public class JanusGraphIoRegistry extends AbstractIoRegistry {

    private static final JanusGraphIoRegistry INSTANCE = new JanusGraphIoRegistry();

    private JanusGraphIoRegistry() {
        register(GraphSONIo.class, null, JanusGraphSONModuleV2d0.getInstance());
        register(GraphBinaryIo.class, RelationIdentifier.class, new RelationIdentifierGraphBinarySerializer());
        register(GraphBinaryIo.class, Geoshape.class, new GeoshapeGraphBinarySerializer());
        register(GraphBinaryIo.class, JanusGraphP.class, new JanusGraphPBinarySerializer());
        register(GryoIo.class, RelationIdentifier.class, null);
        register(GryoIo.class, Geoshape.class, new Geoshape.GeoShapeGryoSerializer());
        register(GryoIo.class, JanusGraphP.class, new JanusGraphPSerializer());
        //fallback for older janusgraph drivers
        register(GryoIo.class, P.class, new DeprecatedJanusGraphPSerializer(new GryoSerializersV3d0.PSerializer()));
    }

    public static JanusGraphIoRegistry instance() {
        return INSTANCE;
    }

    @Deprecated()
    public static JanusGraphIoRegistry getInstance() {
        return instance();
    }
}
