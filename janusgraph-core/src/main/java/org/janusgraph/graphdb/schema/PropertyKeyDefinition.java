package com.thinkaurelius.titan.graphdb.schema;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class PropertyKeyDefinition extends RelationTypeDefinition {

    private final Class<?> dataType;

    public PropertyKeyDefinition(String name, long id, Cardinality cardinality, Class dataType) {
        this(name,id,Multiplicity.convert(cardinality),dataType);
    }

    public PropertyKeyDefinition(String name, long id, Multiplicity multiplicity, Class dataType) {
        super(name, id, multiplicity);
        this.dataType = dataType;
    }

    public PropertyKeyDefinition(PropertyKey key) {
        this(key.name(),key.longId(),key.cardinality(),key.dataType());
    }

    public Class<?> getDataType() {
        return dataType;
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        return dir==Direction.OUT;
    }

}
