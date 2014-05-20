package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.schema.RelationTypeMaker;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeHandling;
import com.thinkaurelius.titan.graphdb.internal.Token;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import org.apache.commons.lang.StringUtils;

import java.util.*;

import static com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory.*;

public abstract class StandardRelationTypeMaker implements RelationTypeMaker {

    static final char[] RESERVED_CHARS = {'{', '}', '"', Token.SEPARATOR_CHAR};

    protected final StandardTitanTx tx;
    protected final IndexSerializer indexSerializer;
    protected final AttributeHandling attributeHandler;

    private String name;
    private boolean isHidden;
    private List<RelationType> sortKey;
    private Order sortOrder;
    private List<RelationType> signature;
    private Multiplicity multiplicity;

    public StandardRelationTypeMaker(final StandardTitanTx tx, final IndexSerializer indexSerializer,
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
        sortKey = new ArrayList<RelationType>(4);
        sortOrder = Order.ASC;
        signature = new ArrayList<RelationType>(4);
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

    private long[] checkSortKey(List<RelationType> sig) {
        for (RelationType t : sig) {
            Preconditions.checkArgument(t.isEdgeLabel()
                    || attributeHandler.isOrderPreservingDatatype(((PropertyKey) t).getDataType()),
                    "Key must have an order-preserving data type to be used as sort key: " + t);
        }
        return checkSignature(sig);
    }

    private static long[] checkSignature(List<RelationType> sig) {
        Preconditions.checkArgument(sig.size() == (Sets.newHashSet(sig)).size(), "Signature and sort key cannot contain duplicate types");
        long[] signature = new long[sig.size()];
        for (int i = 0; i < sig.size(); i++) {
            RelationType et = sig.get(i);
            Preconditions.checkNotNull(et);
            Preconditions.checkArgument(!et.isEdgeLabel() || ((EdgeLabel) et).isUnidirected(),
                    "Label must be unidirectional: %s", et.getName());
            Preconditions.checkArgument(!et.isPropertyKey() || !((PropertyKey) et).getDataType().equals(Object.class),
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
        def.setValue(SIGNATURE, checkSignature(signature));
        def.setValue(MULTIPLICITY,multiplicity);
        return def;
    }

    public StandardRelationTypeMaker multiplicity(Multiplicity multiplicity) {
        Preconditions.checkNotNull(multiplicity);
        this.multiplicity=multiplicity;
        return this;
    }

    public StandardRelationTypeMaker signature(RelationType... types) {
        Preconditions.checkArgument(types!=null && types.length>0);
        signature.addAll(Arrays.asList(types));
        return this;
    }

    public StandardRelationTypeMaker sortKey(RelationType... types) {
        Preconditions.checkArgument(types!=null && types.length>0);
        sortKey.addAll(Arrays.asList(types));
        return this;
    }

    public StandardRelationTypeMaker sortOrder(Order order) {
        Preconditions.checkNotNull(order);
        this.sortOrder=order;
        return this;
    }

    public StandardRelationTypeMaker name(String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name = name;
        return this;
    }

    public StandardRelationTypeMaker hidden() {
        this.isHidden = true;
        return this;
    }


}
