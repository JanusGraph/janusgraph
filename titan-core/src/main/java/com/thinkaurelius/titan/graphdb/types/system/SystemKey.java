package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.Titan;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.internal.TitanTypeCategory;
import com.thinkaurelius.titan.graphdb.types.TypeAttribute;
import com.thinkaurelius.titan.graphdb.types.TypeRelationClassification;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import java.util.Map;

public class SystemKey extends SystemType implements TitanKey {

    public static final SystemKey TypeName =
            new SystemKey("TypeName", String.class, 1, true, new boolean[]{true, true}, false);

    public static final SystemKey TypeDefinition =
            new SystemKey("TypeDefinition", TypeAttribute.class, 2, false, new boolean[]{false, false}, false);

    public static final SystemKey TypeCategory =
            new SystemKey("TypeCategory", TitanTypeCategory.class, 3, true, new boolean[]{true, false}, false);

    public static final SystemKey TypeRelationClassifier =
            new SystemKey("TypeRelationClassifier", TypeRelationClassification.class, 4, false, new boolean[]{true, false}, true);

    public static final SystemKey VertexExists =
            new SystemKey("VertexExists", Boolean.class, 7, false, new boolean[]{true, false}, true);


    public static final Map<String, SystemKey> KEY_MAP = ImmutableMap.of(TypeDefinition.getName(), TypeDefinition,
            TypeName.getName(), TypeName, TypeCategory.getName(), TypeCategory, VertexExists.getName(), VertexExists);

    private final Class<?> dataType;
    private final boolean index;

    private SystemKey(String name, Class<?> dataType, int id) {
        this(name, dataType, id, false, new boolean[]{true, false}, false);
    }

    private SystemKey(String name, Class<?> dataType, int id, boolean index, boolean[] uniqueness, boolean modifiable) {
        super(name, id, RelationCategory.PROPERTY, uniqueness, modifiable);
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
