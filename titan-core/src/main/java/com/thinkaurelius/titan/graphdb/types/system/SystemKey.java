package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.Titan;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.internal.RelationType;
import com.thinkaurelius.titan.graphdb.types.TitanTypeClass;
import com.thinkaurelius.titan.graphdb.types.TypeAttribute;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import java.util.Map;

public class SystemKey extends SystemType implements TitanKey {

    public static final SystemKey TypeName =
            new SystemKey("TypeName", String.class, 1, true, new boolean[]{true, true}, false);

    public static final SystemKey TypeDefinition =
            new SystemKey("TypeDefinition", TypeAttribute.class, 2, false, new boolean[]{false, false}, false);

    public static final SystemKey TypeClass =
            new SystemKey("TypeClass", TitanTypeClass.class, 3, true, new boolean[]{true, false}, false);

    public static final SystemKey VertexState =
            new SystemKey("VertexState", Byte.class, 4, false, new boolean[]{true, false}, true);

    public enum VertexStates {
        DEFAULT(0);

        private byte value;

        VertexStates(int value) {
            Preconditions.checkArgument(value >= 0 && value <= Byte.MAX_VALUE);
            this.value = (byte) value;
        }

        public byte getValue() {
            return value;
        }
    }

    public static final Map<String, SystemKey> KEY_MAP = ImmutableMap.of(TypeDefinition.getName(), TypeDefinition,
            TypeName.getName(), TypeName, TypeClass.getName(), TypeClass, VertexState.getName(), VertexState);

    private final Class<?> dataType;
    private final boolean index;

    private SystemKey(String name, Class<?> dataType, int id) {
        this(name, dataType, id, false, new boolean[]{true, false}, false);
    }

    private SystemKey(String name, Class<?> dataType, int id, boolean index, boolean[] uniqueness, boolean modifiable) {
        super(name, id, RelationType.PROPERTY, uniqueness, new boolean[]{!modifiable, uniqueness[1] && index}, modifiable);
        this.dataType = dataType;
        this.index = index;
    }


    @Override
    public Class<?> getDataType() {
        return dataType;
    }

    @Override
    public Iterable<String> getIndexes(Class<? extends Element> elementType) {
        if (index && elementType == Vertex.class) return ImmutableList.of(Titan.Token.STANDARD_INDEX);
        else return IterablesUtil.emptyIterable();
    }

    @Override
    public boolean hasIndex(String name, Class<? extends Element> elementType) {
        return elementType == Vertex.class && index && Titan.Token.STANDARD_INDEX.equals(name);
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
