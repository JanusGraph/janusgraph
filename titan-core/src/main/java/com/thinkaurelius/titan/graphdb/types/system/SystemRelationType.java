package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.ConsistencyModifier;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import org.apache.commons.lang.StringUtils;

public abstract class SystemRelationType extends EmptyRelationType implements InternalRelationType {

    private final String name;
    private final long id;


    SystemRelationType(String name, long id, RelationCategory type) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name = SystemTypeManager.systemETprefix + name;
        this.id = getSystemTypeId(id, type);
    }

    static long getSystemTypeId(long id, RelationCategory type) {
        Preconditions.checkArgument(id > 0);
        Preconditions.checkArgument(id < SystemTypeManager.SYSTEM_RELATIONTYPE_OFFSET, "System id [%s] is too large", id);
        Preconditions.checkArgument(type.isProper());
        switch (type) {
            case EDGE:
                return IDManager.getSchemaId(IDManager.VertexIDType.EdgeLabel,id);
            case PROPERTY:
                return IDManager.getSchemaId(IDManager.VertexIDType.PropertyKey,id);
            default:
                throw new AssertionError("Illegal condition: " + type);
        }
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
    public boolean isHiddenRelationType() {
        return true;
    }



}
