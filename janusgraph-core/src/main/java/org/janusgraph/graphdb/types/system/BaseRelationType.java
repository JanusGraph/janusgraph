package org.janusgraph.graphdb.types.system;

import com.google.common.base.Preconditions;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.JanusSchemaCategory;
import org.janusgraph.graphdb.internal.Token;
import org.apache.commons.lang.StringUtils;

public abstract class BaseRelationType extends EmptyRelationType implements SystemRelationType {

    private final String name;
    private final long id;


    BaseRelationType(String name, long id, JanusSchemaCategory type) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name = Token.systemETprefix + name;
        this.id = getSystemTypeId(id, type);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long longId() {
        return id;
    }

    @Override
    public boolean hasId() {
        return true;
    }

    @Override
    public void setId(long id) {
        throw new IllegalStateException("SystemType has already been assigned an id");
    }

    @Override
    public ConsistencyModifier getConsistencyModifier() {
        return ConsistencyModifier.LOCK;
    }

    @Override
    public boolean isInvisibleType() {
        return true;
    }


    static long getSystemTypeId(long id, JanusSchemaCategory type) {
        Preconditions.checkArgument(id > 0);
        Preconditions.checkArgument(type.isRelationType());
        switch (type) {
            case EDGELABEL:
                return IDManager.getSchemaId(IDManager.VertexIDType.SystemEdgeLabel, id);
            case PROPERTYKEY:
                return IDManager.getSchemaId(IDManager.VertexIDType.SystemPropertyKey,id);
            default:
                throw new AssertionError("Illegal argument: " + type);
        }
    }

}
