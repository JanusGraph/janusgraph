package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.tinkerpop.io.graphson.TitanGraphSONModule;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TitanIoRegistry extends AbstractIoRegistry {

    public static TitanIoRegistry INSTANCE = new TitanIoRegistry();

    // todo: made the constructor temporarily public to workaround an interoperability issue with hadoop in tp3 GA https://issues.apache.org/jira/browse/TINKERPOP3-771

    public TitanIoRegistry() {
        register(GraphSONIo.class, null, TitanGraphSONModule.getInstance());
        register(GryoIo.class, RelationIdentifier.class, null);
        register(GryoIo.class, Geoshape.class, new Geoshape.GeoShapeGryoSerializer());
    }
}
