package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.Titan;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.types.PropertyKeyDefinition;
import com.thinkaurelius.titan.graphdb.types.StandardKeyDefinition;
import com.thinkaurelius.titan.graphdb.types.StandardLabelDefinition;
import com.thinkaurelius.titan.graphdb.types.TitanTypeClass;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

public class SystemKey extends SystemType implements PropertyKeyDefinition, TitanKey {

    public static final SystemKey PropertyKeyDefinition =
            new SystemKey("PropertyKeyDefinition", StandardKeyDefinition.class, 2);

    public static final SystemKey RelationTypeDefinition =
            new SystemKey("EdgeLabelDefinition", StandardLabelDefinition.class, 3);

    public static final SystemKey TypeName =
            new SystemKey("TypeName", String.class, 4, true, true, false);

    public static final SystemKey TypeClass =
            new SystemKey("TypeClass", TitanTypeClass.class, 6, true, false, false);

    public static final SystemKey VertexState =
            new SystemKey("VertexState", Byte.class, 7, false, false, true);

    public static final Iterable<SystemKey> values() {
        return ImmutableList.of(PropertyKeyDefinition, RelationTypeDefinition, TypeName, TypeClass, VertexState);
    }

    private final Class<?> dataType;
    private final boolean index;

    private SystemKey(String name, Class<?> dataType, int id) {
        this(name, dataType, id, false, false, false);
    }

    private SystemKey(String name, Class<?> dataType, int id, boolean index, boolean unique, boolean modifiable) {
        super(name,id,new boolean[]{true,unique},new boolean[]{!modifiable,unique && index},modifiable);
        this.dataType = dataType;
        this.index = index;
    }

    @Override
    public Class<?> getDataType() {
        return dataType;
    }

    @Override
    public Iterable<String> getIndexes(Class<? extends Element> elementType) {
        if (index && elementType==Vertex.class) return ImmutableList.of(Titan.Token.STANDARD_INDEX);
        else return IterablesUtil.emptyIterable();
    }

    @Override
    public boolean hasIndex(String name, Class<? extends Element> elementType) {
        return elementType==Vertex.class && index && Titan.Token.STANDARD_INDEX.equals(name);
    }

    @Override
    public final boolean isPropertyKey() {
        return true;
    }

    @Override
    public final boolean isEdgeLabel() {
        return false;
    }


}
