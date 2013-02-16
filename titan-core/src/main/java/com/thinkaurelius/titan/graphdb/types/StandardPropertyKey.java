package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TypeGroup;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import java.util.Collection;
import java.util.List;

public class StandardPropertyKey extends AbstractTypeDefinition implements PropertyKeyDefinition {

    private IndexType[] indexes;
    private boolean isVertexUnique;
    private Class<?> objectType;

    private transient List<IndexType> vertexIndexes;
    private transient List<IndexType> edgeIndexes;

    public StandardPropertyKey() {
    }

    public StandardPropertyKey(String name, TypeCategory category,
                               Directionality directionality, TypeVisibility visibility,
                               FunctionalType isfunctional, String[] keysig,
                               String[] compactsig, TypeGroup group,
                               boolean isVertexUnique, IndexType[] indexes, Class<?> objectType) {
        super(name, category, directionality, visibility, isfunctional,
                keysig, compactsig, group);
        Preconditions.checkArgument(objectType != null);
        this.indexes=indexes;
        this.isVertexUnique = isVertexUnique;
        this.objectType = objectType;
    }

    @Override
    public Class<?> getDataType() {
        return objectType;
    }

    private List<IndexType> getIndexList(Class<? extends Element> type) {
        Preconditions.checkArgument(type==Vertex.class || type==Edge.class, "Expected Vertex oe Edge class as argument");
        List<IndexType> result = type==Vertex.class?vertexIndexes:edgeIndexes;
        if (result==null) {
            //Build it
            ImmutableList.Builder b = new ImmutableList.Builder();
            for (IndexType it : indexes) if (type.isAssignableFrom(it.getElementType())) b.add(it);
            result = b.build();
            if (type==Vertex.class) vertexIndexes=result;
            else if (type==Edge.class) edgeIndexes=result;
        }
        return result;
    }

    @Override
    public boolean hasIndex(Class<? extends Element> clazz) {
        return !getIndexList(clazz).isEmpty();
    }

    @Override
    public Collection<IndexType> getIndexes(Class<? extends Element> clazz) {
        return getIndexList(clazz);
    }

    @Override
    public boolean isUnique() {
        return isVertexUnique;
    }

}
