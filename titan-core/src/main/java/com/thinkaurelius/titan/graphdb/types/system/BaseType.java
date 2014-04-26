package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.ConsistencyModifier;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import org.apache.commons.lang.StringUtils;

public abstract class BaseType extends EmptyType implements SystemType {

    private final String name;
    private final long id;


    BaseType(String name, long id, TitanSchemaCategory type) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name = SystemTypeManager.systemETprefix + name;
        this.id = getSystemTypeId(id, type);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getID() {
        return id;
    }

    @Override
    public boolean hasId() {
        return true;
    }

    @Override
    public void setID(long id) {
        throw new IllegalStateException("SystemType has already been assigned an id");
    }

    @Override
    public ConsistencyModifier getConsistencyModifier() {
        return ConsistencyModifier.LOCK;
    }

    @Override
    public boolean isHiddenType() {
        return true;
    }


    static long getSystemTypeId(long id, TitanSchemaCategory type) {
        Preconditions.checkArgument(id > 0);
        Preconditions.checkArgument(type.isRelationType());
        switch (type) {
            case LABEL:
                return IDManager.getSchemaId(IDManager.VertexIDType.SystemEdgeLabel, id);
            case KEY:
                return IDManager.getSchemaId(IDManager.VertexIDType.SystemPropertyKey,id);
            default:
                throw new AssertionError("Illegal argument: " + type);
        }
    }

}
