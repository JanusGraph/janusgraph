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
import org.apache.commons.lang.StringUtils;

import java.util.*;

import static com.thinkaurelius.titan.graphdb.types.TypeAttributeType.*;
import static com.tinkerpop.blueprints.Direction.OUT;

abstract class StandardTypeMaker implements TypeMaker {

    private static final Set<String> RESERVED_NAMES = ImmutableSet.of("id", "label");//, "key");

    private static final char[] RESERVED_CHARS = {'{', '}', '"'};

    protected final StandardTitanTx tx;
    protected final IndexSerializer indexSerializer;

    private String name;
    private boolean[] isUnique;
    private boolean[] hasUniqueLock;
    private boolean[] isStatic;
    private boolean isHidden;
    private boolean isModifiable;
    private List<TitanType> sortKey;
    private List<TitanType> signature;

    public StandardTypeMaker(final StandardTitanTx tx, final IndexSerializer indexSerializer) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(indexSerializer);
        this.tx = tx;
        this.indexSerializer = indexSerializer;

        //Default assignments
        name = null;
        isUnique = new boolean[2]; //false
        hasUniqueLock = new boolean[2]; //false
        isStatic = new boolean[2]; //false
        isHidden = false;
        isModifiable = true;
        sortKey = new ArrayList<TitanType>(4);
        signature = new ArrayList<TitanType>(4);
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
        checkSortKey(sortKey);
        checkSignature(signature);
        Preconditions.checkArgument(Sets.intersection(Sets.newHashSet(sortKey), Sets.newHashSet(signature)).isEmpty(),
                "Signature and sort key must be disjoined");
        if ((isUnique[0] && isUnique[1]) && !sortKey.isEmpty())
            throw new IllegalArgumentException("Cannot define a sort key on a both-unique type");
    }

    private static long[] checkSortKey(List<TitanType> sig) {
        for (TitanType t : sig) {
            Preconditions.checkArgument(t.isEdgeLabel()
                    || Comparable.class.isAssignableFrom(((TitanKey) t).getDataType()),
                    "Key must have comparable data type to be used as sort key: " + t);
        }
        return checkSignature(sig);
    }

    private static long[] checkSignature(List<TitanType> sig) {
        Preconditions.checkArgument(sig.size() == (Sets.newHashSet(sig)).size(), "Signature and sort key cannot contain duplicate types");
        long[] signature = new long[sig.size()];
        for (int i = 0; i < sig.size(); i++) {
            TitanType et = sig.get(i);
            Preconditions.checkNotNull(et);
            Preconditions.checkArgument(et.isUnique(OUT), "Type must be single valued: %s", et.getName());
            Preconditions.checkArgument(!et.isEdgeLabel() || ((TitanLabel) et).isUnidirected(),
                    "Label must be unidirectional: %s", et.getName());
            Preconditions.checkArgument(!et.isPropertyKey() || !((TitanKey) et).getDataType().equals(Object.class),
                    "Signature and sort keys must have a proper declared datatype: %s", et.getName());
            signature[i] = et.getID();
        }
        return signature;
    }

    protected final TypeAttribute.Map makeDefinition() {
        checkGeneralArguments();

        TypeAttribute.Map def = new TypeAttribute.Map();
        def.setValue(UNIQUENESS, new boolean[]{isUnique[0], isUnique[1]});
        def.setValue(UNIQUENESS_LOCK, hasUniqueLock);
        def.setValue(STATIC, isStatic);
        def.setValue(HIDDEN, isHidden);
        def.setValue(MODIFIABLE, isModifiable);
        def.setValue(SORT_KEY, checkSortKey(sortKey));
        def.setValue(SIGNATURE, checkSignature(signature));
        return def;
    }

    protected StandardTypeMaker signature(TitanType... types) {
        signature.addAll(Arrays.asList(types));
        return this;
    }

    protected StandardTypeMaker sortKey(TitanType... types) {
        sortKey.addAll(Arrays.asList(types));
        return this;
    }

    public StandardTypeMaker name(String name) {
        this.name = name;
        return this;
    }

    protected String getName() {
        return this.name;
    }

    protected boolean isUnique(Direction direction) {
        Preconditions.checkArgument(direction == Direction.IN || direction == Direction.OUT);
        return isUnique[EdgeDirection.position(direction)];
    }

    protected StandardTypeMaker unique(Direction direction, UniquenessConsistency consistency) {
        if (direction == Direction.BOTH) {
            unique(Direction.IN, consistency);
            unique(Direction.OUT, consistency);
        } else {
            isUnique[EdgeDirection.position(direction)] = consistency == null ? false : true;
            hasUniqueLock[EdgeDirection.position(direction)] =
                    (consistency == UniquenessConsistency.LOCK ? true : false);
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

    protected boolean isStatic(Direction direction) {
        Preconditions.checkArgument(direction == Direction.IN || direction == Direction.OUT);
        return isStatic[EdgeDirection.position(direction)];
    }


}
