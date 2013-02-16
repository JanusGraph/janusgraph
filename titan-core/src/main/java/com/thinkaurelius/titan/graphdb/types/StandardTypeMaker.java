package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.core.TypeMaker;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.types.manager.TypeManager;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import java.util.*;

public class StandardTypeMaker implements TypeMaker {

    private static final Set<String> RESERVED_NAMES = ImmutableSet.of("id", "label");

    private final InternalTitanTransaction tx;
    private final TypeManager etManager;

    private TypeVisibility visibility;
    private String name;
    private TypeGroup group;
    private FunctionalType isFunctional;
    private Directionality directionality;
    private TypeCategory category;
    private List<TitanType> primarySig;
    private List<TitanType> compactsig;

    private Set<IndexType> indexes;
    private boolean isVertexUnique;
    private Class<?> objectType;

    public StandardTypeMaker(InternalTitanTransaction tx, TypeManager etManager) {
        this.tx = tx;
        this.etManager = etManager;

        //Default assignments
        objectType = null;
        name = null;
        indexes = new HashSet<IndexType>(4);
        isVertexUnique = false;
        primarySig = new ArrayList<TitanType>();
        compactsig = new ArrayList<TitanType>();
        category = null;
        directionality = Directionality.Directed;
        isFunctional = FunctionalType.NON_FUNCTIONAL;
        group = TypeGroup.DEFAULT_GROUP;
        visibility = TypeVisibility.Modifiable;
    }

    private void checkGeneralArguments() {
        if (name == null || name.length() == 0)
            throw new IllegalArgumentException("Need to specify name");
        if (name.startsWith(SystemTypeManager.systemETprefix))
            throw new IllegalArgumentException("Name starts with a reserved keyword: " + SystemTypeManager.systemETprefix);
        if (RESERVED_NAMES.contains(name.toLowerCase()))
            throw new IllegalArgumentException("Name is reserved: " + name);
        if ((!primarySig.isEmpty() || !compactsig.isEmpty()) && category != TypeCategory.HasProperties)
            throw new IllegalArgumentException("Can only specify signatures for labeled edge types");
        checkPrimarySignature(primarySig);
        checkSignature(compactsig);
        Set<TitanType> intersectSign = Sets.newHashSet(primarySig);
        intersectSign.retainAll(compactsig);
        if (!intersectSign.isEmpty())
            throw new IllegalArgumentException("The primary key and the compact signature contain identical types: " + intersectSign);
    }

    private TitanType[] checkPrimarySignature(List<TitanType> sig) {
        for (TitanType t : sig) {
            Preconditions.checkArgument(t.isEdgeLabel()
                    || Comparable.class.isAssignableFrom(((TitanKey) t).getDataType()),
                    "Type must be a label or a key with comparable data type: " + t);
        }
        return checkSignature(sig);
    }

    private TitanType[] checkSignature(List<TitanType> sig) {
        TitanType[] signature = new TitanType[sig.size()];
        for (int i = 0; i < sig.size(); i++) {
            TitanType et = sig.get(i);
            if (!et.isFunctional())
                throw new IllegalArgumentException("Signature edge types must be functional :" + et);
            if (!et.isSimple())
                throw new IllegalArgumentException("Signature edge types must be simple: " + et);
            if (et.isEdgeLabel() && !((TitanLabel) et).isUnidirected())
                throw new IllegalArgumentException("Signature relationship types must be unidirected: " + et);
            if (et.isPropertyKey() && ((TitanKey) et).getDataType().equals(Object.class))
                throw new IllegalArgumentException("Signature keys must have a proper declared datatype: " + et);
            signature[i] = et;
        }
        return signature;
    }

    private IndexType[] checkIndexes(Set<IndexType> indexes) {
        IndexType[] result = new IndexType[indexes.size()];
        int i =0;
        for (IndexType it : indexes) {
            result[i]=it;
            i++;
        }
        return result;
    }

    @Override
    public TitanKey makePropertyKey() {
        if (category == null) category = TypeCategory.Simple;
        checkGeneralArguments();
        if (directionality != Directionality.Directed)
            throw new IllegalArgumentException("keys must be directed");
        if (category != TypeCategory.Simple)
            throw new IllegalArgumentException("Only simple properties are supported");
        if (objectType == null)
            throw new IllegalArgumentException("Need to specify data type");
        if (isVertexUnique && !indexes.contains(IndexType.of(Vertex.class)))
            throw new IllegalArgumentException("A unique key requires the existence of a standard vertex index");
        return etManager.createPropertyKey(tx, name, category, directionality,
                visibility, isFunctional, checkSignature(primarySig), checkSignature(compactsig),
                group, isVertexUnique, checkIndexes(indexes), objectType);
    }

    @Override
    public TitanLabel makeEdgeLabel() {
        if (category == null) category = TypeCategory.HasProperties;
        checkGeneralArguments();
        if (!indexes.isEmpty())
            throw new IllegalArgumentException("Cannot declare labels to be indexed");
        return etManager.createEdgeLabel(tx, name, category, directionality,
                visibility, isFunctional, checkPrimarySignature(primarySig), checkSignature(compactsig), group);

    }

    @Override
    public StandardTypeMaker signature(TitanType... types) {
        compactsig = Arrays.asList(types);
        return this;
    }

    @Override
    public StandardTypeMaker primaryKey(TitanType... types) {
        primarySig = Arrays.asList(types);
        return this;
    }

    @Override
    public StandardTypeMaker simple() {
        category = TypeCategory.Simple;
        return this;
    }

    @Override
    public StandardTypeMaker dataType(Class<?> clazz) {
        objectType = clazz;
        return this;
    }

    @Override
    public StandardTypeMaker functional() {
        return functional(true);
    }

    @Override
    public StandardTypeMaker functional(boolean locking) {
        if (locking) isFunctional = FunctionalType.FUNCTIONAL_LOCKING;
        else isFunctional = FunctionalType.FUNCTIONAL;
        return this;
    }

    @Override
    public TypeMaker directed() {
        directionality = Directionality.Directed;
        return this;
    }

    @Override
    public TypeMaker undirected() {
        directionality = Directionality.Undirected;
        return this;
    }

    @Override
    public TypeMaker unidirected() {
        directionality = Directionality.Unidirected;
        return this;
    }

    @Override
    public StandardTypeMaker group(TypeGroup group) {
        this.group = group;
        return this;
    }

    @Override
    public StandardTypeMaker name(String name) {
        this.name = name;
        return this;
    }

    @Override
    public TypeMaker unique() {
        isVertexUnique = true;
        return this;
    }

    @Override
    public TypeMaker indexed(Class<? extends Element> clazz) {
        if (clazz==Element.class) {
            this.indexes.add(IndexType.of(Vertex.class));
            this.indexes.add(IndexType.of(Edge.class));
        } else {
            this.indexes.add(IndexType.of(clazz));
        }
        return this;
    }

    @Override
    public TypeMaker indexed(String indexName, Class<? extends Element> clazz) {
        if (clazz==Element.class) {
            this.indexes.add(IndexType.of(indexName, Vertex.class));
            this.indexes.add(IndexType.of(indexName, Edge.class));
        } else {
            this.indexes.add(IndexType.of(indexName, clazz));
        }
        return this;
    }

}
