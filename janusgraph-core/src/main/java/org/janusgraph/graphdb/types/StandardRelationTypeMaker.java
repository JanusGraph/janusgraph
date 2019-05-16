// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.RelationTypeMaker;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.serialize.AttributeHandler;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.system.SystemTypeManager;

import java.util.*;

import static org.janusgraph.graphdb.types.TypeDefinitionCategory.*;

public abstract class StandardRelationTypeMaker implements RelationTypeMaker {

    protected final StandardJanusGraphTx tx;
    protected final IndexSerializer indexSerializer;
    protected final AttributeHandler attributeHandler;

    private String name;
    private boolean isInvisible;
    private final List<PropertyKey> sortKey;
    private Order sortOrder;
    private final List<PropertyKey> signature;
    private Multiplicity multiplicity;
    private SchemaStatus status = SchemaStatus.ENABLED;

    public StandardRelationTypeMaker(final StandardJanusGraphTx tx, String name,
                                     final IndexSerializer indexSerializer,
                                     final AttributeHandler attributeHandler) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(indexSerializer);
        Preconditions.checkNotNull(attributeHandler);
        this.tx = tx;
        this.indexSerializer = indexSerializer;
        this.attributeHandler = attributeHandler;
        name(name);

        //Default assignments
        isInvisible = false;
        sortKey = new ArrayList<>(4);
        sortOrder = Order.ASC;
        signature = new ArrayList<>(4);
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

    abstract JanusGraphSchemaCategory getSchemaCategory();

    private void checkGeneralArguments() {
        checkSortKey(sortKey);
        Preconditions.checkArgument(sortOrder==Order.ASC || hasSortKey(),"Must define a sort key to use ordering");
        checkSignature(signature);
        Preconditions.checkArgument(Sets.intersection(Sets.newHashSet(sortKey), Sets.newHashSet(signature)).isEmpty(),
                "Signature and sort key must be disjoined");
        Preconditions.checkArgument(!hasSortKey() || !multiplicity.isConstrained(),"Cannot define a sort-key on constrained edge labels");
    }

    private long[] checkSortKey(List<PropertyKey> sig) {
        for (PropertyKey key : sig) {
            Preconditions.checkArgument(attributeHandler.isOrderPreservingDatatype(key.dataType()),
                    "Key must have an order-preserving data type to be used as sort key: " + key);
        }
        return checkSignature(sig);
    }

    private static long[] checkSignature(List<PropertyKey> sig) {
        Preconditions.checkArgument(sig.size() == (Sets.newHashSet(sig)).size(), "Signature and sort key cannot contain duplicate types");
        long[] signature = new long[sig.size()];
        for (int i = 0; i < sig.size(); i++) {
            PropertyKey key = sig.get(i);
            Preconditions.checkNotNull(key);
            Preconditions.checkArgument(!((PropertyKey) key).dataType().equals(Object.class),
                    "Signature and sort keys must have a proper declared datatype: %s", key.name());
            signature[i] = key.longId();
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
    public StandardRelationTypeMaker signature(PropertyKey... types) {
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
     * <p>
     * Specifying the sort key of a type allows relations of this type to be efficiently retrieved in the order of
     * the sort key.
     * <br>
     * For instance, if the edge label <i>friend</i> has the sort key (<i>since</i>), which is a property key
     * with a timestamp data type, then one can efficiently retrieve all edges with label <i>friend</i> in a specified
     * time interval using {@link org.janusgraph.core.JanusGraphVertexQuery#interval}.
     * <br>
     * In other words, relations are stored on disk in the order of the configured sort key. The sort key is empty
     * by default.
     * <br>
     * If multiple types are specified as sort key, then those are considered as a <i>composite</i> sort key, i.e. taken jointly
     * in the given order.
     * <p>
     * {@link org.janusgraph.core.RelationType}s used in the sort key must be either property out-unique keys or out-unique unidirected edge lables.
     *
     * @param keys JanusGraphTypes composing the sort key. The order is relevant.
     * @return this LabelMaker
     */
    public StandardRelationTypeMaker sortKey(PropertyKey... keys) {
        Preconditions.checkArgument(keys!=null && keys.length>0);
        sortKey.addAll(Arrays.asList(keys));
        return this;
    }

    /**
     * Defines in which order to sort the relations for efficient retrieval, i.e. either increasing ({@link org.janusgraph.graphdb.internal.Order#ASC}) or
     * decreasing ({@link org.janusgraph.graphdb.internal.Order#DESC}).
     *
     * Note, that only one sort order can be specified and that a sort key must be defined to use a sort order.
     *
     * @param order
     * @return
     * @see #sortKey(PropertyKey... keys)
     */
    public StandardRelationTypeMaker sortOrder(Order order) {
        Preconditions.checkNotNull(order);
        this.sortOrder=order;
        return this;
    }

    public StandardRelationTypeMaker name(String name) {
        SystemTypeManager.throwIfSystemName(getSchemaCategory(), name);
        this.name = name;
        return this;
    }

    public StandardRelationTypeMaker invisible() {
        this.isInvisible = true;
        return this;
    }


}
