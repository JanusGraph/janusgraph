package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.graphdb.schema.*;

import java.util.concurrent.atomic.AtomicInteger;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public class TestSchemaProvider {

    public static final SchemaProvider MULTIPLICITY = new MultiplicitySchemaProvider(false);

    public static final SchemaProvider MULTIPLICITY_ID = new MultiplicitySchemaProvider(true);

    private static class MultiplicitySchemaProvider implements SchemaProvider {

        private final boolean assignIds;
        private final AtomicInteger idCounter = new AtomicInteger(0);

        private MultiplicitySchemaProvider(boolean assignIds) {
            this.assignIds = assignIds;
        }

        private long getId() {
            if (assignIds) return idCounter.incrementAndGet();
            else return FaunusElement.NO_ID;
        }

        @Override
        public EdgeLabelDefinition getEdgeLabel(String name) {
            Multiplicity multi = Multiplicity.MULTI;
            if (name.startsWith("m2o")) multi = Multiplicity.MANY2ONE;
            else if (name.startsWith("o2m")) multi = Multiplicity.ONE2MANY;
            else if (name.startsWith("o2o")) multi = Multiplicity.ONE2ONE;
            else if (name.startsWith("simple")) multi = Multiplicity.SIMPLE;
            return new EdgeLabelDefinition(name, getId(), multi, false);
        }

        @Override
        public PropertyKeyDefinition getPropertyKey(String name) {
            Cardinality card = Cardinality.SINGLE;
            if (name.endsWith("list")) card=Cardinality.LIST;
            else if (name.endsWith("set")) card=Cardinality.SET;
            return new PropertyKeyDefinition(name, getId(), card, Object.class);
        }

        @Override
        public VertexLabelDefinition getVertexLabel(String name) {
            return new VertexLabelDefinition(name, getId(),false,false);
        }

        @Override
        public RelationTypeDefinition getRelationType(String name) {
            return null;
        }
    }

}
