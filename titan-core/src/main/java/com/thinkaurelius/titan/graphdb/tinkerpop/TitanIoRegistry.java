package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.tinkerpop.io.graphson.TitanGraphSONModule;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;

/**
 *
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanIoRegistry extends AbstractIoRegistry {

    public static TitanIoRegistry INSTANCE = new TitanIoRegistry();

    private TitanIoRegistry() {
        register(GraphSONIo.class, null, TitanGraphSONModule.getInstance());
        register(GryoIo.class, RelationIdentifier.class, null);
    }




}
