package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.schema.RelationTypeMaker;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeHandling;
import com.thinkaurelius.titan.graphdb.internal.Order;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import com.thinkaurelius.titan.graphdb.internal.Token;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import org.apache.commons.lang.StringUtils;

import java.util.*;

import static com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory.*;

public abstract class StandardRelationTypeMaker implements RelationTypeMaker {

    static final char[] RESERVED_CHARS = {'{', '}', '"', Token.SEPARATOR_CHAR};

    protected final StandardTitanTx tx;
    protected final IndexSerializer indexSerializer;
    protected final AttributeHandling attributeHandler;

    private String name;
    private boolean isInvisible;
    private List<RelationType> sortKey;
    private Order sortOrder;
    private List<RelationType> signature;
    private Multiplicity multiplicity;
    private SchemaStatus status = SchemaStatus.ENABLED;

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
        isInvisible = false;
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

    abstract TitanSchemaCategory getSchemaCategory();

    public static void checkName(TitanSchemaCategory category, String name) {
        TypeUtil.checkTypeName(category,name);
        if (SystemTypeManager.isSystemType(name.toLowerCase())
                || Token.isSystemName(name)) {
            if (category==TitanSchemaCategory.EDGELABEL) {
                throw Element.Exceptions.labelCanNotBeASystemKey(name);
            } else {
                throw Property.Exceptions.propertyKeyCanNotBeASystemKey(name);
            }
        }
        for (char c : RESERVED_CHARS)
            Preconditions.checkArgument(name.indexOf(c) < 0, "Name can not contains reserved character %s: %s", c, name);
    }

    private void checkGeneralArguments() {
        //Verify name
        checkName(getSchemaCategory(),name);
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
                    || attributeHandler.isOrderPreservingDatatype(((PropertyKey) t).dataType()),
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
                    "Label must be unidirectional: %s", et.name());
            Preconditions.checkArgument(!et.isPropertyKey() || !((PropertyKey) et).dataType().equals(Object.class),
                    "Signature and sort keys must have a proper declared datatype: %s", et.name());
            signature[i] = et.longId();
        }
        return signature;
    }

    protected final TypeDefinitionMap makeDefinition() {
        checkGeneralArguments();

        TypeDefinitionMap def = new TypeDefinitionMap();
        def.setValue(INVISIBLE, isInvisible);
        def.setValue(SORT_KEY, checkSortKey(sortKey));
        def.setValue(SORT_ORDER, sortOrder);
        def.setValue(SIGNATURE, checkSignature(signature));
        def.setValue(MULTIPLICITY,multiplicity);
        def.setValue(STATUS,status);
        return def;
    }

    public StandardRelationTypeMaker multiplicity(Multiplicity multiplicity) {
        Preconditions.checkNotNull(multiplicity);
        this.multiplicity=multiplicity;
        return this;
    }

    @Override
    public StandardRelationTypeMaker signature(RelationType... types) {
        Preconditions.checkArgument(types!=null && types.length>0);
        signature.addAll(Arrays.asList(types));
        return this;
    }

    public StandardRelationTypeMaker status(SchemaStatus status) {
        Preconditions.checkArgument(status!=null);
        this.status=status;
        return this;
    }

    /**
     * Configures the composite sort key for this label.
     * <p/>
     * Specifying the sort key of a type allows relations of this type to be efficiently retrieved in the order of
     * the sort key.
     * <br />
     * For instance, if the edge label <i>friend</i> has the sort key (<i>since</i>), which is a property key
     * with a timestamp data type, then one can efficiently retrieve all edges with label <i>friend</i> in a specified
     * time interval using {@link com.thinkaurelius.titan.core.TitanVertexQuery#interval(com.thinkaurelius.titan.core.PropertyKey, Comparable, Comparable)}.
     * <br />
     * In other words, relations are stored on disk in the order of the configured sort key. The sort key is empty
     * by default.
     * <br />
     * If multiple types are specified as sort key, then those are considered as a <i>composite</i> sort key, i.e. taken jointly
     * in the given order.
     * <p/>
     * {@link com.thinkaurelius.titan.core.RelationType}s used in the sort key must be either property out-unique keys or out-unique unidirected edge lables.
     *
     * @param types TitanTypes composing the sort key. The order is relevant.
     * @return this LabelMaker
     */
    public StandardRelationTypeMaker sortKey(RelationType... types) {
        Preconditions.checkArgument(types!=null && types.length>0);
        sortKey.addAll(Arrays.asList(types));
        return this;
    }

    /**
     * Defines in which order to sort the relations for efficient retrieval, i.e. either increasing ({@link com.thinkaurelius.titan.graphdb.internal.Order#ASC}) or
     * decreasing ({@link com.thinkaurelius.titan.graphdb.internal.Order#DESC}).
     *
     * Note, that only one sort order can be specified and that a sort key must be defined to use a sort order.
     *
     * @param order
     * @return
     * @see #sortKey(RelationType...)
     */
    public StandardRelationTypeMaker sortOrder(Order order) {
        Preconditions.checkNotNull(order);
        this.sortOrder=order;
        return this;
    }

    public StandardRelationTypeMaker name(String name) {
        this.name = name;
        return this;
    }

    public StandardRelationTypeMaker invisible() {
        this.isInvisible = true;
        return this;
    }


}
