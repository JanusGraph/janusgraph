package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.core.TypeMaker;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.lang.StringUtils;

import java.util.*;

public class StandardTypeMaker implements TypeMaker {

    private static final Set<String> RESERVED_NAMES = ImmutableSet.of("id", "label", "key");

    private final StandardTitanTx tx;

    private String name;
    private TypeGroup group;
    private boolean[] isUnique;
    private boolean[] hasUniqueLock;
    private boolean[] isStatic;
    private boolean isHidden;
    private boolean isModifiable;
    private List<TitanType> primaryKey;
    private List<TitanType> signature;

    private boolean isUnidirectional;

    private Set<IndexType> indexes;
    private Class<?> dataType;


    public StandardTypeMaker(StandardTitanTx tx) {
        this.tx = tx;

        //Default assignments
        name = null;
        group = TypeGroup.DEFAULT_GROUP;
        isUnique = new boolean[2]; //false
        hasUniqueLock = new boolean[2]; //false
        isStatic = new boolean[2]; //false
        isHidden = false;
        isModifiable = true;
        primaryKey = new ArrayList<TitanType>(4);
        signature = new ArrayList<TitanType>(4);

        isUnidirectional=false;

        indexes = new HashSet<IndexType>(4);
        dataType = null;
    }

    private void checkGeneralArguments() {
        Preconditions.checkArgument(StringUtils.isBlank(name),"Need to specify name");
        Preconditions.checkArgument(!name.startsWith(SystemTypeManager.systemETprefix),
            "Name starts with a reserved keyword: " + SystemTypeManager.systemETprefix);
        Preconditions.checkArgument(!RESERVED_NAMES.contains(name.toLowerCase()),
            "Name is reserved: " + name);
        for (int i=0;i<2;i++) Preconditions.checkArgument(!hasUniqueLock[i] || isUnique[i],
                "Must be unique in order to have a lock");
        checkPrimaryKey(primaryKey);
        checkSignature(signature);
        Preconditions.checkArgument(Sets.intersection(Sets.newHashSet(primaryKey),Sets.newHashSet(signature)).isEmpty(),
                "Signature and primary key must be disjoined");
    }

    private static long[] checkPrimaryKey(List<TitanType> sig) {
        for (TitanType t : sig) {
            Preconditions.checkArgument(t.isEdgeLabel()
                    || Comparable.class.isAssignableFrom(((TitanKey) t).getDataType()),
                    "Key must have comparable data type to be used as primary key: " + t);
        }
        return checkSignature(sig);
    }

    private static long[] checkSignature(List<TitanType> sig) {
        Preconditions.checkArgument(sig.size() == (Sets.newHashSet(sig)).size(),"Signature and primary key cannot contain duplicate types");
        long[] signature = new long[sig.size()];
        for (int i = 0; i < sig.size(); i++) {
            TitanType et = sig.get(i);
            Preconditions.checkNotNull(et);
            Preconditions.checkArgument(et.isUnique(Direction.OUT),"Type must be functional: " + et);
            Preconditions.checkArgument(!et.isEdgeLabel() || ((TitanLabel)et).isUnidirected(),
                    "Label must be unidirectional: " + et);
            Preconditions.checkArgument(!et.isPropertyKey() || !((TitanKey) et).getDataType().equals(Object.class),
                    "Signature keys must have a proper declared datatype: " + et);
            signature[i] = et.getID();
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
        checkGeneralArguments();
        isUnidirectional=false;
        Preconditions.checkNotNull(dataType,"Need to specify a datatype");
        Preconditions.checkArgument(!isUnique[EdgeDirection.position(Direction.OUT)] ||
                indexes.contains(IndexType.of(Vertex.class)), "A unique key requires the existence of a standard vertex index");
        return tx.makePropertyKey(new StandardKeyDefinition(name,group,isUnique,hasUniqueLock,isStatic,isHidden,isModifiable,
                checkPrimaryKey(primaryKey),checkSignature(signature),checkIndexes(indexes),dataType));

    }


    @Override
    public TitanLabel makeEdgeLabel() {
        checkGeneralArguments();
        Preconditions.checkArgument(indexes.isEmpty(),"Cannot declare labels to be indexed");
        return tx.makeEdgeLabel(new StandardLabelDefinition(name, group, isUnique, hasUniqueLock, isStatic, isHidden, isModifiable,
                checkPrimaryKey(primaryKey), checkSignature(signature), isUnidirectional));
    }

    @Override
    public StandardTypeMaker signature(TitanType... types) {
        signature.addAll(Arrays.asList(types));
        return this;
    }

    @Override
    public StandardTypeMaker primaryKey(TitanType... types) {
        primaryKey.addAll(Arrays.asList(types));
        return this;
    }


    @Override
    public StandardTypeMaker dataType(Class<?> clazz) {
        Preconditions.checkNotNull(clazz);
        dataType = clazz;
        return this;
    }


    @Override
    public StandardTypeMaker directed() {
        isUnidirectional=false;
        return this;
    }

    @Override
    public StandardTypeMaker unidirected() {
        isUnidirectional=true;
        return this;
    }

    @Override
    public StandardTypeMaker group(TypeGroup group) {
        Preconditions.checkNotNull(group);
        this.group = group;
        return this;
    }

    @Override
    public StandardTypeMaker name(String name) {
        this.name = name;
        return this;
    }

    @Override
    public StandardTypeMaker unique(Direction direction, UniquenessConsistency consistency) {
        if (direction==Direction.BOTH) {
            unique(Direction.IN,consistency); unique(Direction.OUT,consistency);
        } else {
            isUnique[EdgeDirection.position(direction)]=true;
            hasUniqueLock[EdgeDirection.position(direction)]=
                    (consistency==UniquenessConsistency.LOCK?true:false);
        }
        return this;
    }

    @Override
    public StandardTypeMaker unique(Direction direction) {
        unique(direction,UniquenessConsistency.LOCK);
        return this;
    }

    @Override
    public StandardTypeMaker indexed(Class<? extends Element> clazz) {
        if (clazz==Element.class) {
            this.indexes.add(IndexType.of(Vertex.class));
            this.indexes.add(IndexType.of(Edge.class));
        } else {
            this.indexes.add(IndexType.of(clazz));
        }
        return this;
    }

    @Override
    public StandardTypeMaker indexed(String indexName, Class<? extends Element> clazz) {
        if (clazz==Element.class) {
            this.indexes.add(IndexType.of(indexName, Vertex.class));
            this.indexes.add(IndexType.of(indexName, Edge.class));
        } else {
            this.indexes.add(IndexType.of(indexName, clazz));
        }
        return this;
    }

    public StandardTypeMaker hidden() {
        this.isHidden=true;
        this.isModifiable=false;
        return this;
    }


    public StandardTypeMaker unModifiable() {
        this.isModifiable=false;
        return this;
    }

    public StandardTypeMaker makeStatic(Direction direction) {
        if (direction==Direction.BOTH) {
            makeStatic(Direction.IN); makeStatic(Direction.OUT);
        } else {
            isStatic[EdgeDirection.position(direction)]=true;
        }
        return this;
    }

}
