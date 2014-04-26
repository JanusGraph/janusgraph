package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ImplicitKey extends EmptyType implements SystemType, TitanKey {

    public static final ImplicitKey ID = new ImplicitKey(0,"id",Long.class);

    public static final ImplicitKey LABEL = new ImplicitKey(0,"label",Long.class);

//    public static final ImplicitKey KEY = new ImplicitKey("key",Long.class);

    //######### IMPLICIT KEYS WITH ID ############

    public static final ImplicitKey TIMESTAMP = new ImplicitKey(8,"_timestamp",Long.class);

    public static final ImplicitKey VISIBILITY = new ImplicitKey(9,"_visibility",String.class);

    private final Class<?> datatype;
    private final String name;
    private final long id;

    private ImplicitKey(final long id, final String name, final Class<?> datatype) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name) && datatype!=null && id>=0);
        this.datatype=datatype;
        this.name=name;
        if (id>0) {
            this.id=BaseType.getSystemTypeId(id, TitanSchemaCategory.KEY);
        } else {
            this.id=-1;
        }
    }


    @Override
    public Class<?> getDataType() {
        return datatype;
    }

    @Override
    public Cardinality getCardinality() {
        return Cardinality.SINGLE;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isPropertyKey() {
        return true;
    }

    @Override
    public boolean isEdgeLabel() {
        return false;
    }

    @Override
    public boolean isHiddenType() {
        return false;
    }

    @Override
    public Multiplicity getMultiplicity() {
        return Multiplicity.convert(getCardinality());
    }

    @Override
    public ConsistencyModifier getConsistencyModifier() {
        return ConsistencyModifier.DEFAULT;
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        return dir==Direction.OUT;
    }

    @Override
    public long getID() {
        Preconditions.checkArgument(hasId());
        return id;
    }

    @Override
    public boolean hasId() {
        return id>0;
    }

    @Override
    public void setID(long id) {
        throw new IllegalStateException("SystemType has already been assigned an id");
    }

}
