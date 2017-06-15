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

import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.tinkerpop.io.graphson.JanusGraphSONModule;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JanusGraphIoRegistry extends AbstractIoRegistry {

    private static JanusGraphIoRegistry INSTANCE = new JanusGraphIoRegistry();

    // todo: made the constructor temporarily public to workaround an interoperability issue with hadoop in tp3 GA https://issues.apache.org/jira/browse/TINKERPOP3-771

    public JanusGraphIoRegistry() {
        register(GraphSONIo.class, null, JanusGraphSONModule.getInstance());
        register(GryoIo.class, RelationIdentifier.class, null);
        register(GryoIo.class, Geoshape.class, new Geoshape.GeoShapeGryoSerializer());
        register(GryoIo.class, P.class, new JanusGraphPSerializer());
    }

    public static JanusGraphIoRegistry getInstance() {
        return INSTANCE;
    }
}
