package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.tinkerpop.io.graphson.TitanGraphSONModule;
import com.tinkerpop.gremlin.structure.io.DefaultIo;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import com.tinkerpop.gremlin.structure.io.kryo.KryoMapper;

/**
 *
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanIo extends DefaultIo {

    public TitanIo(TitanGraph graph) {
        super(graph);
    }

    @Override
    public KryoMapper.Builder kryoMapper() {
        //Add user registered serializers
        return KryoMapper.build().addCustom(RelationIdentifier.class);
    }

    @Override
    public GraphSONMapper.Builder graphSONMapper() {
        return GraphSONMapper.build().addCustomModule(TitanGraphSONModule.getInstance());
    }

}
