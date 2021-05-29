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

package org.janusgraph.graphdb.tinkerpop.io.graphson;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.tinkerpop.io.JanusGraphP;

/**
 * Supports GraphSON 2.0
 */
public class JanusGraphSONModuleV2d0 extends JanusGraphSONModule {

    private JanusGraphSONModuleV2d0() {
        super();
        addSerializer(RelationIdentifier.class, new RelationIdentifierSerializerV2d0());
        addSerializer(Geoshape.class, new Geoshape.GeoshapeGsonSerializerV2d0());
        addSerializer(JanusGraphP.class, new JanusGraphPSerializerV2d0());

        addDeserializer(RelationIdentifier.class, new RelationIdentifierDeserializerV2d0());
        addDeserializer(Geoshape.class, new Geoshape.GeoshapeGsonDeserializerV2d0());
        addDeserializer(JanusGraphP.class, new JanusGraphPDeserializerV2d0());
        //fallback for older janusgraph drivers
        addDeserializer(P.class, new DeprecatedJanusGraphPDeserializerV2d0());
    }

    private static final JanusGraphSONModuleV2d0 INSTANCE = new JanusGraphSONModuleV2d0();

    public static JanusGraphSONModuleV2d0 getInstance() {
        return INSTANCE;
    }

}
