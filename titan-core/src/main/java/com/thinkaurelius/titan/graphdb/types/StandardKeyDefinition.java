package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TypeGroup;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

public class StandardKeyDefinition extends AbstractTypeDefinition implements PropertyKeyDefinition {

    private IndexType[] indexes;
    private Class<?> objectType;

    private transient List<IndexType> vertexIndexes;
    private transient List<IndexType> edgeIndexes;

    StandardKeyDefinition() {
    }

    StandardKeyDefinition(String name, TypeGroup group,
                                 boolean[] unique, boolean[] hasUniqueLock, boolean[] isStatic,
                                 boolean hidden, boolean modifiable,
                                 long[] primaryKey, long[] signature,
                                 IndexType[] indexes, Class<?> objectType) {
        super(name, group, unique, hasUniqueLock, isStatic, hidden, modifiable, primaryKey, signature);
        this.indexes = indexes;
        this.objectType = objectType;
    }

    @Override
    public Class<?> getDataType() {
        return objectType;
    }

    @Override
    public Iterable<String> getIndexes(Class<? extends Element> clazz) {
        if (clazz==Vertex.class || clazz==Edge.class) return Iterables.transform(getIndexList(clazz),new Function<IndexType, String>() {
            @Nullable
            @Override
            public String apply(@Nullable IndexType indexType) {
                return indexType.getIndexName();
            }
        });
        else if (clazz == Element.class) return Iterables.concat(getIndexes(Vertex.class),getIndexes(Edge.class));
        else throw new IllegalArgumentException("Unexpected element type: " + clazz);
    }

    @Override
    public boolean hasIndex(String name, Class<? extends Element> elementType) {
        Preconditions.checkArgument(elementType==Vertex.class || elementType==Edge.class, "Expected Vertex or Edge class as argument");
        for (int i=0;i<indexes.length;i++) {
            if (indexes[i].getElementType()==elementType && indexes[i].getIndexName().equals(name)) return true;
        }
        return false;
    }

    private List<IndexType> getIndexList(Class<? extends Element> type) {
        Preconditions.checkArgument(type==Vertex.class || type==Edge.class, "Expected Vertex or Edge class as argument");
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


}
