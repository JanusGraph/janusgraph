package com.thinkaurelius.titan.graphdb.types.vertices;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.thinkaurelius.titan.graphdb.types.TypeAttributeType;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import javax.annotation.Nullable;
import java.util.List;

public class TitanKeyVertex extends TitanTypeVertex implements TitanKey {

    public TitanKeyVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public Class<?> getDataType() {
        return getDefinition().getValue(TypeAttributeType.DATATYPE,Class.class);
    }

    @Override
    public final boolean isPropertyKey() {
        return true;
    }

    @Override
    public final boolean isEdgeLabel() {
        return false;
    }

    private final IndexType[] getIndexes() {
        if (indexes==null) {
            indexes = getDefinition().getValue(TypeAttributeType.INDEXES,IndexType[].class);
        }
        Preconditions.checkNotNull(indexes);
        return indexes;
    }

    private transient IndexType[] indexes;
    private transient List<IndexType> vertexIndexes;
    private transient List<IndexType> edgeIndexes;

    @Override
    public Iterable<String> getIndexes(Class<? extends Element> clazz) {
        if (clazz==Vertex.class || clazz==Edge.class) {
            if (getIndexList(clazz).isEmpty()) return ImmutableList.of();
            else return Iterables.transform(getIndexList(clazz), new Function<IndexType, String>() {
                @Nullable
                @Override
                public String apply(@Nullable IndexType indexType) {
                    return indexType.getIndexName();
                }
            });
        } else if (clazz == Element.class) return Iterables.concat(getIndexes(Vertex.class),getIndexes(Edge.class));
        else throw new IllegalArgumentException("Unexpected element type: " + clazz);
    }

    @Override
    public boolean hasIndex(String name, Class<? extends Element> elementType) {
        Preconditions.checkArgument(elementType==Vertex.class || elementType==Edge.class, "Expected Vertex or Edge class as argument");
        for (int i=0;i<getIndexes().length;i++) {
            if (getIndexes()[i].getElementType()==elementType && getIndexes()[i].getIndexName().equals(name)) return true;
        }
        return false;
    }

    private List<IndexType> getIndexList(Class<? extends Element> type) {
        Preconditions.checkArgument(type==Vertex.class || type==Edge.class, "Expected Vertex or Edge class as argument");
        List<IndexType> result = type==Vertex.class?vertexIndexes:edgeIndexes;
        if (result==null) {
            //Build it
            ImmutableList.Builder b = new ImmutableList.Builder();
            for (IndexType it : getIndexes()) if (type.isAssignableFrom(it.getElementType())) b.add(it);
            result = b.build();
            if (type==Vertex.class) vertexIndexes=result;
            else if (type==Edge.class) edgeIndexes=result;
        }
        return result;
    }

}
