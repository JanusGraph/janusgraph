package com.thinkaurelius.titan.graphdb.types.vertices;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.Parameter;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.IndexDefinition;
import com.thinkaurelius.titan.graphdb.types.IndexParameters;
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

    private IndexDefinition[] getIndexes() {
        if (indexes==null) {
            IndexType[] indexTypes = getDefinition().getValue(TypeAttributeType.INDEXES,IndexType[].class);
            IndexParameters[] indexParas = getDefinition().getValue(TypeAttributeType.INDEX_PARAMETERS,IndexParameters[].class);
            Preconditions.checkArgument(indexTypes!=null,"Missing index types!");
            if (indexParas==null) { //Default initialization to no parameters
                indexParas = new IndexParameters[indexTypes.length];
                for (int i=0;i<indexTypes.length;i++)
                    indexParas[i]=new IndexParameters(indexTypes[i].getIndexName(),new Parameter[0]);
            }
            Preconditions.checkArgument(indexTypes.length==indexParas.length,"Lengths don't agree: %s vs %s",indexTypes.length,indexParas.length);
            IndexDefinition tmp[] = new IndexDefinition[indexTypes.length];
            for (int i=0;i<indexTypes.length;i++) {
                tmp[i]=IndexDefinition.of(indexTypes[i],indexParas[i]);
            }
            indexes = tmp;
        }
        Preconditions.checkNotNull(indexes);
        return indexes;
    }

    private volatile transient IndexDefinition[] indexes;
    private volatile transient List<IndexDefinition> vertexIndexes;
    private volatile transient List<IndexDefinition> edgeIndexes;

    @Override
    public Iterable<String> getIndexes(Class<? extends Element> clazz) {
        if (clazz==Vertex.class || clazz==Edge.class) {
            if (getIndexList(clazz).isEmpty()) return ImmutableList.of();
            else return Iterables.transform(getIndexList(clazz), new Function<IndexDefinition, String>() {
                @Nullable
                @Override
                public String apply(@Nullable IndexDefinition indexType) {
                    return indexType.getIndexName();
                }
            });
        } else if (clazz == Element.class) return Iterables.concat(getIndexes(Vertex.class),getIndexes(Edge.class));
        else throw new IllegalArgumentException("Unexpected element type: " + clazz);
    }

    @Override
    public boolean hasIndex(String name, Class<? extends Element> elementType) {
        return getIndex(name,elementType)!=null;
    }

    public IndexDefinition getIndex(String name, Class<? extends Element> elementType) {
        Preconditions.checkArgument(elementType==Vertex.class || elementType==Edge.class, "Expected Vertex or Edge class as argument");
        for (int i=0;i<getIndexes().length;i++) {
            IndexDefinition def = getIndexes()[i];
            if (def.getElementType()==elementType && def.getIndexName().equals(name)) return def;
        }
        return null;
    }

    private List<IndexDefinition> getIndexList(Class<? extends Element> type) {
        Preconditions.checkArgument(type==Vertex.class || type==Edge.class, "Expected Vertex or Edge class as argument");
        List<IndexDefinition> result = type==Vertex.class?vertexIndexes:edgeIndexes;
        if (result==null) {
            //Build it
            ImmutableList.Builder b = new ImmutableList.Builder();
            for (IndexDefinition it : getIndexes())
                if (type.isAssignableFrom(it.getElementType()))
                    b.add(it);
            result = b.build();
            if (type==Vertex.class) vertexIndexes=result;
            else if (type==Edge.class) edgeIndexes=result;
        }
        return result;
    }

}
