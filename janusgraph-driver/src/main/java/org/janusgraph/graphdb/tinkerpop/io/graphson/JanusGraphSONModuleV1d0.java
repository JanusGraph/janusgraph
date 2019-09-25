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

import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.graphdb.relations.RelationIdentifier;

/**
 * @author Stephen Mallette (https://stephen.genoprime.com)
 */
public class JanusGraphSONModuleV1d0 extends JanusGraphSONModule {

    private JanusGraphSONModuleV1d0() {
        super();
        addSerializer(RelationIdentifier.class, new RelationIdentifierSerializerV1d0());
        addSerializer(Geoshape.class, new Geoshape.GeoshapeGsonSerializerV1d0());

        addDeserializer(RelationIdentifier.class, new RelationIdentifierDeserializerV1d0());
        addDeserializer(Geoshape.class, new Geoshape.GeoshapeGsonDeserializerV1d0());
    }

    private static final JanusGraphSONModuleV1d0 INSTANCE = new JanusGraphSONModuleV1d0();

    public static JanusGraphSONModuleV1d0 getInstance() {
        return INSTANCE;
    }

}
