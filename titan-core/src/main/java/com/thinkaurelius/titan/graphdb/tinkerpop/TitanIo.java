package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.tinkerpop.io.graphson.TitanGraphSONModule;
import org.apache.tinkerpop.gremlin.structure.io.DefaultIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;

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
    public GryoMapper.Builder gryoMapper() {
        //Add user registered serializers
        return GryoMapper.build().addCustom(RelationIdentifier.class);
    }

    @Override
    public GraphSONMapper.Builder graphSONMapper() {
        return GraphSONMapper.build().addCustomModule(TitanGraphSONModule.getInstance());
    }

}
