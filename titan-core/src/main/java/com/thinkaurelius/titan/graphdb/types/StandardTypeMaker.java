package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeMaker;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Modifier;
import java.util.*;

import static com.thinkaurelius.titan.graphdb.types.TypeAttributeType.*;
import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

public class StandardTypeMaker implements TypeMaker {

    private static final Set<String> RESERVED_NAMES = ImmutableSet.of("id", "label");//, "key");

    private static final char[] RESERVED_CHARS = {'{', '}'};

    private final StandardTitanTx tx;
    private final IndexSerializer indexSerializer;

    private String name;
    private Boolean[] isUnique; //null values indicate no-choice => use defaults
    private boolean[] hasUniqueLock;
    private boolean[] isStatic;
    private boolean isHidden;
    private boolean isModifiable;
    private List<TitanType> primaryKey;
    private List<TitanType> signature;

    private boolean isUnidirectional;

    private Class<?> dataType;
    private Set<IndexType> indexes;

    public StandardTypeMaker(final StandardTitanTx tx, final IndexSerializer indexSerializer) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(indexSerializer);
        this.tx = tx;
        this.indexSerializer = indexSerializer;

        //Default assignments
        name = null;
        isUnique = new Boolean[2]; //null
        hasUniqueLock = new boolean[2]; //false
        isStatic = new boolean[2]; //false
        isHidden = false;
        isModifiable = true;
        primaryKey = new ArrayList<TitanType>(4);
        signature = new ArrayList<TitanType>(4);

        isUnidirectional = false;

        indexes = new HashSet<IndexType>(4);
        dataType = null;
    }

    private void checkGeneralArguments() {
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "Need to specify name");
        for (char c : RESERVED_CHARS)
            Preconditions.checkArgument(name.indexOf(c) < 0, "Name can not contains reserved character %s: %s", c, name);
        Preconditions.checkArgument(!name.startsWith(SystemTypeManager.systemETprefix),
                "Name starts with a reserved keyword: " + SystemTypeManager.systemETprefix);
        Preconditions.checkArgument(!RESERVED_NAMES.contains(name.toLowerCase()),
                "Name is reserved: " + name);

        for (int i = 0; i < 2; i++)
            Preconditions.checkArgument(!hasUniqueLock[i] || isUnique[i],
                    "Must be unique in order to have a lock");
        checkPrimaryKey(primaryKey);
        checkSignature(signature);
        Preconditions.checkArgument(Sets.intersection(Sets.newHashSet(primaryKey), Sets.newHashSet(signature)).isEmpty(),
                "Signature and primary key must be disjoined");
        if ((isUnique[0] && isUnique[1]) && !primaryKey.isEmpty())
            throw new IllegalArgumentException("Cannot define a primary key on a both-unique type");
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
        Preconditions.checkArgument(sig.size() == (Sets.newHashSet(sig)).size(), "Signature and primary key cannot contain duplicate types");
        long[] signature = new long[sig.size()];
        for (int i = 0; i < sig.size(); i++) {
            TitanType et = sig.get(i);
            Preconditions.checkNotNull(et);
            Preconditions.checkArgument(et.isUnique(OUT), "Type must be out-unique: %s", et.getName());
            Preconditions.checkArgument(!et.isEdgeLabel() || ((TitanLabel) et).isUnidirected(),
                    "Label must be unidirectional: %s", et.getName());
            Preconditions.checkArgument(!et.isPropertyKey() || !((TitanKey) et).getDataType().equals(Object.class),
                    "Signature keys must have a proper declared datatype: %s", et.getName());
            signature[i] = et.getID();
        }
        return signature;
    }

    private IndexType[] checkIndexes(Set<IndexType> indexes) {
        IndexType[] result = new IndexType[indexes.size()];
        int i = 0;
        for (IndexType it : indexes) {
            Preconditions.checkArgument(isUnique[EdgeDirection.position(OUT)] || (it.isStandardIndex() && it.getElementType() == Vertex.class),
                    "Only standard index is allowed on non-unique property keys");
            Preconditions.checkArgument(indexSerializer.getIndexInformation(it.getIndexName()).supports(dataType), "" +
                    "Index [" + it.getIndexName() + "] does not support data type: " + dataType);
            result[i] = it;
            i++;
        }
        return result;
    }


    private final TypeAttribute.Map makeDefinition() {
        TypeAttribute.Map def = new TypeAttribute.Map();
        def.setValue(UNIQUENESS, new boolean[]{isUnique[0], isUnique[1]});
        def.setValue(UNIQUENESS_LOCK, hasUniqueLock);
        def.setValue(STATIC, isStatic);
        def.setValue(HIDDEN, isHidden);
        def.setValue(MODIFIABLE, isModifiable);
        def.setValue(PRIMARY_KEY, checkPrimaryKey(primaryKey));
        def.setValue(SIGNATURE, checkSignature(signature));
        return def;
    }

    @Override
    public TitanKey makePropertyKey() {
        //Make default assignments
        isUnidirectional = false;
        if (isUnique[EdgeDirection.position(OUT)] == null) isUnique[EdgeDirection.position(OUT)] = true;
        if (isUnique[EdgeDirection.position(IN)] == null) isUnique[EdgeDirection.position(IN)] = false;

        checkGeneralArguments();
        Preconditions.checkArgument(dataType != null, "Need to specify a datatype");
        Preconditions.checkArgument(!dataType.isPrimitive(), "Primitive types are not supported. Use the corresponding object type, e.g. Integer.class instead of int.class [%s]", dataType);
        Preconditions.checkArgument(!dataType.isInterface(), "Datatype must be a class and not an interface: %s", dataType);
        Preconditions.checkArgument(dataType.isArray() || !Modifier.isAbstract(dataType.getModifiers()), "Datatype cannot be an abstract class: %s", dataType);
        Preconditions.checkArgument(!isUnique[EdgeDirection.position(IN)] ||
                indexes.contains(IndexType.of(Vertex.class)), "A graph-unique key requires the existence of a standard vertex index");

        TypeAttribute.Map definition = makeDefinition();
        definition.setValue(DATATYPE, dataType).setValue(INDEXES, checkIndexes(indexes));
        return tx.makePropertyKey(name, definition);
    }


    @Override
    public TitanLabel makeEdgeLabel() {
        //Make default assignments
        if (isUnique[EdgeDirection.position(OUT)] == null) isUnique[EdgeDirection.position(OUT)] = false;
        if (isUnique[EdgeDirection.position(IN)] == null) isUnique[EdgeDirection.position(IN)] = false;

        checkGeneralArguments();
        Preconditions.checkArgument(indexes.isEmpty(), "Cannot declare labels to be indexed");
        Preconditions.checkArgument(dataType == null, "Cannot declare a data type for a label");

        Preconditions.checkArgument(!isUnidirectional ||
                (!isUnique[EdgeDirection.position(IN)] && !hasUniqueLock[EdgeDirection.position(IN)] && !isStatic[EdgeDirection.position(IN)]),
                "Unidirectional labels cannot be unique or static");

        TypeAttribute.Map definition = makeDefinition();
        definition.setValue(UNIDIRECTIONAL, isUnidirectional);
        return tx.makeEdgeLabel(name, definition);
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
        Preconditions.checkArgument(clazz != null, "Need to specify a data type");
        dataType = clazz;
        return this;
    }


    @Override
    public StandardTypeMaker directed() {
        isUnidirectional = false;
        return this;
    }

    @Override
    public StandardTypeMaker unidirected() {
        isUnidirectional = true;
        return this;
    }

    @Override
    public StandardTypeMaker name(String name) {
        this.name = name;
        return this;
    }

    private StandardTypeMaker unique(Direction direction, UniquenessConsistency consistency) {
        if (direction == Direction.BOTH) {
            unique(Direction.IN, consistency);
            unique(Direction.OUT, consistency);
        } else {
            isUnique[EdgeDirection.position(direction)] = true;
            hasUniqueLock[EdgeDirection.position(direction)] =
                    (consistency == UniquenessConsistency.LOCK ? true : false);
        }
        return this;
    }

    @Override
    public StandardTypeMaker vertexUnique(Direction direction, UniquenessConsistency consistency) {
        return unique(direction, consistency);
    }

    @Override
    public StandardTypeMaker vertexUnique(Direction direction) {
        vertexUnique(direction, UniquenessConsistency.LOCK);
        return this;
    }

    @Override
    public StandardTypeMaker multiValued() {
        isUnique[EdgeDirection.position(OUT)] = false;
        hasUniqueLock[EdgeDirection.position(OUT)] = false;
        return this;
    }

    @Override
    public StandardTypeMaker graphUnique() {
        return unique(Direction.IN, UniquenessConsistency.LOCK);
    }

    @Override
    public StandardTypeMaker graphUnique(UniquenessConsistency consistency) {
        return unique(Direction.IN, consistency);
    }

    @Override
    public StandardTypeMaker indexed(Class<? extends Element> clazz) {
        if (clazz == Element.class) {
            this.indexes.add(IndexType.of(Vertex.class));
            this.indexes.add(IndexType.of(Edge.class));
        } else {
            this.indexes.add(IndexType.of(clazz));
        }
        return this;
    }

    @Override
    public StandardTypeMaker indexed(String indexName, Class<? extends Element> clazz) {
        if (clazz == Element.class) {
            this.indexes.add(IndexType.of(indexName, Vertex.class));
            this.indexes.add(IndexType.of(indexName, Edge.class));
        } else {
            this.indexes.add(IndexType.of(indexName, clazz));
        }
        return this;
    }

    public StandardTypeMaker hidden() {
        this.isHidden = true;
        return this;
    }


    public StandardTypeMaker unModifiable() {
        this.isModifiable = false;
        return this;
    }

    public StandardTypeMaker makeStatic(Direction direction) {
        if (direction == Direction.BOTH) {
            makeStatic(Direction.IN);
            makeStatic(Direction.OUT);
        } else {
            isStatic[EdgeDirection.position(direction)] = true;
        }
        return this;
    }

}
