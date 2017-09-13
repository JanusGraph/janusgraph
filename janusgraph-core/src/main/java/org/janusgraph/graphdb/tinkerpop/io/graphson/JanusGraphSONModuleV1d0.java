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

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JanusGraphSONModuleV1d0 extends JanusGraphSONModule {

    private JanusGraphSONModuleV1d0() {
        super();
        addSerializer(Geoshape.class, new Geoshape.GeoshapeGsonSerializerV1d0());

        addDeserializer(Geoshape.class, new Geoshape.GeoshapeGsonDeserializerV1d0());
    }

    private static final JanusGraphSONModuleV1d0 INSTANCE = new JanusGraphSONModuleV1d0();

    public static final JanusGraphSONModuleV1d0 getInstance() {
        return INSTANCE;
    }

}
