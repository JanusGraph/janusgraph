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
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoSerializersV1d0;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.tinkerpop.io.JanusGraphP;
import org.janusgraph.graphdb.tinkerpop.io.graphson.JanusGraphSONModuleV1d0;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Stephen Mallette (https://stephen.genoprime.com)
 */
public class JanusGraphIoRegistryV1d0 extends AbstractIoRegistry {

    private static final JanusGraphIoRegistryV1d0 INSTANCE = new JanusGraphIoRegistryV1d0();

    private JanusGraphIoRegistryV1d0() {
        register(GraphSONIo.class, null, JanusGraphSONModuleV1d0.getInstance());
        register(GryoIo.class, RelationIdentifier.class, null);
        register(GryoIo.class, Geoshape.class, new Geoshape.GeoShapeGryoSerializer());
        register(GryoIo.class, JanusGraphP.class, new JanusGraphPSerializer());
        //fallback for older JanusGraph drivers
        register(GryoIo.class, P.class, new DeprecatedJanusGraphPSerializer(new GryoSerializersV1d0.PSerializer()));
    }

    public static JanusGraphIoRegistryV1d0 instance() {
        return INSTANCE;
    }

    @Deprecated()
    public static JanusGraphIoRegistryV1d0 getInstance() {
        return instance();
    }
}
