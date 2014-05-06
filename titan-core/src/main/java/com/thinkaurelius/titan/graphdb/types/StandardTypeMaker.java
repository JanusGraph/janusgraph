package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeHandling;
import com.thinkaurelius.titan.graphdb.internal.Token;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.ImplicitKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import org.apache.commons.lang.StringUtils;

import java.util.*;

import static com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory.*;

public abstract class StandardTypeMaker implements TypeMaker {

    private static final char[] RESERVED_CHARS = {'{', '}', '"'};

    protected final StandardTitanTx tx;
    protected final IndexSerializer indexSerializer;
    protected final AttributeHandling attributeHandler;

    private String name;
    private boolean isHidden;
    private List<TitanType> sortKey;
    private Order sortOrder;
    private int ttl;
    private List<TitanType> signature;
    private Multiplicity multiplicity;

    public StandardTypeMaker(final StandardTitanTx tx, final IndexSerializer indexSerializer,
                             final AttributeHandling attributeHandler) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(indexSerializer);
        Preconditions.checkNotNull(attributeHandler);
        this.tx = tx;
        this.indexSerializer = indexSerializer;
        this.attributeHandler = attributeHandler;

        //Default assignments
        name = null;
        isHidden = false;
        sortKey = new ArrayList<TitanType>(4);
        sortOrder = Order.ASC;
        ttl = 0;
        signature = new ArrayList<TitanType>(4);
        multiplicity = Multiplicity.MULTI;
    }


    public String getName() {
        return this.name;
    }

    protected boolean hasSortKey() {
        return !sortKey.isEmpty();
    }

    protected Multiplicity getMultiplicity() {
        return multiplicity;
    }

    private void checkGeneralArguments() {
        //Verify name
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "Need to specify name");
        for (char c : RESERVED_CHARS)
            Preconditions.checkArgument(name.indexOf(c) < 0, "Name can not contains reserved character %s: %s", c, name);
        if (!isHidden) Token.verifyName(name);
        Preconditions.checkArgument(!name.startsWith(SystemTypeManager.systemETprefix),
                "Name starts with a reserved keyword: " + SystemTypeManager.systemETprefix);
        Preconditions.checkArgument(!SystemTypeManager.isSystemType(name.toLowerCase()),
                "Name is reserved by system and cannot be used: %s",name);

        checkSortKey(sortKey);
        Preconditions.checkArgument(sortOrder==Order.ASC || hasSortKey(),"Must define a sort key to use ordering");
        checkSignature(signature);
        Preconditions.checkArgument(Sets.intersection(Sets.newHashSet(sortKey), Sets.newHashSet(signature)).isEmpty(),
                "Signature and sort key must be disjoined");
        Preconditions.checkArgument(!hasSortKey() || !multiplicity.isConstrained(),"Cannot define a sort-key on constrained edge labels");
    }

    private long[] checkSortKey(List<TitanType> sig) {
        for (TitanType t : sig) {
            Preconditions.checkArgument(t.isEdgeLabel()
                    || attributeHandler.isOrderPreservingDatatype(((TitanKey) t).getDataType()),
                    "Key must have an order-preserving data type to be used as sort key: " + t);
        }
        return checkSignature(sig);
    }

    private static long[] checkSignature(List<TitanType> sig) {
        Preconditions.checkArgument(sig.size() == (Sets.newHashSet(sig)).size(), "Signature and sort key cannot contain duplicate types");
        long[] signature = new long[sig.size()];
        for (int i = 0; i < sig.size(); i++) {
            TitanType et = sig.get(i);
            Preconditions.checkNotNull(et);
            Preconditions.checkArgument(!et.isEdgeLabel() || ((TitanLabel) et).isUnidirected(),
                    "Label must be unidirectional: %s", et.getName());
            Preconditions.checkArgument(!et.isPropertyKey() || !((TitanKey) et).getDataType().equals(Object.class),
                    "Signature and sort keys must have a proper declared datatype: %s", et.getName());
            signature[i] = et.getID();
        }
        return signature;
    }

    protected final TypeDefinitionMap makeDefinition() {
        checkGeneralArguments();

        TypeDefinitionMap def = new TypeDefinitionMap();
        def.setValue(HIDDEN, isHidden);
        def.setValue(SORT_KEY, checkSortKey(sortKey));
        def.setValue(SORT_ORDER, sortOrder);
        def.setValue(TTL, ttl);
        def.setValue(SIGNATURE, checkSignature(signature));
        def.setValue(MULTIPLICITY,multiplicity);
        return def;
    }

    public StandardTypeMaker multiplicity(Multiplicity multiplicity) {
        Preconditions.checkNotNull(multiplicity);
        this.multiplicity=multiplicity;
        return this;
    }

    public StandardTypeMaker signature(TitanType... types) {
        Preconditions.checkArgument(types!=null && types.length>0);
        signature.addAll(Arrays.asList(types));
        return this;
    }

    public StandardTypeMaker sortKey(TitanType... types) {
        Preconditions.checkArgument(types!=null && types.length>0);
        sortKey.addAll(Arrays.asList(types));
        return this;
    }

    public StandardTypeMaker sortOrder(Order order) {
        Preconditions.checkNotNull(order);
        this.sortOrder=order;
        return this;
    }

    protected StandardTypeMaker ttl(Integer seconds) {
        Preconditions.checkArgument(seconds >= 1, "ttl must be at least one second");
        ttl = seconds;
        return this;
    }

    public StandardTypeMaker name(String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name = name;
        return this;
    }

    public StandardTypeMaker hidden() {
        this.isHidden = true;
        return this;
    }


}
