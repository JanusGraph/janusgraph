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
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.PropertyKeyMaker;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.serialize.AttributeHandler;
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

import java.lang.reflect.Modifier;

import static org.janusgraph.graphdb.types.TypeDefinitionCategory.DATATYPE;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardPropertyKeyMaker extends StandardRelationTypeMaker implements PropertyKeyMaker {

    private Class<?> dataType;
    private boolean cardinalityIsSet;

    public StandardPropertyKeyMaker(StandardJanusGraphTx tx, String name, IndexSerializer indexSerializer,
                                    final AttributeHandler attributeHandler) {
        super(tx, name, indexSerializer, attributeHandler);
        cardinality(Cardinality.SINGLE);
        cardinalityIsSet = false;
    }

    @Override
    JanusGraphSchemaCategory getSchemaCategory() {
        return JanusGraphSchemaCategory.PROPERTYKEY;
    }

    @Override
    public StandardPropertyKeyMaker dataType(Class<?> clazz) {
        Preconditions.checkArgument(clazz != null, "Need to specify a data type");
        dataType = clazz;
        return this;
    }

    @Override
    public StandardPropertyKeyMaker cardinality(Cardinality cardinality) {
        super.multiplicity(Multiplicity.convert(cardinality));
        cardinalityIsSet = true;
        return this;
    }

    @Override
    public StandardPropertyKeyMaker cardinality(VertexProperty.Cardinality cardinality) {
        super.multiplicity(Multiplicity.convert(cardinality));
        cardinalityIsSet = true;
        return this;
    }

    @Override
    public boolean cardinalityIsSet() {
        return cardinalityIsSet;
    }

    @Override
    public StandardPropertyKeyMaker invisible() {
        super.invisible();
        return this;
    }

    @Override
    public StandardPropertyKeyMaker signature(PropertyKey... types) {
        super.signature(types);
        return this;
    }

    @Override
    public StandardPropertyKeyMaker sortKey(PropertyKey... types) {
        super.sortKey(types);
        return this;
    }

    @Override
    public StandardPropertyKeyMaker sortOrder(Order order) {
        super.sortOrder(order);
        return this;
    }

    @Override
    public PropertyKey make() {
        Preconditions.checkArgument(dataType != null, "Need to specify a datatype");
        Preconditions.checkArgument(tx.validDataType(dataType), "Not a supported data type: %s",dataType);
        Preconditions.checkArgument(!dataType.isPrimitive(), "Primitive types are not supported. Use the corresponding wrapper class, e.g. Integer.class instead of int.class [%s]", dataType);
        Preconditions.checkArgument(!dataType.isInterface(), "Datatype must be a class and not an interface: %s", dataType);
        Preconditions.checkArgument(dataType.isArray() || !Modifier.isAbstract(dataType.getModifiers()), "Datatype cannot be an abstract class: %s", dataType);

        TypeDefinitionMap definition = makeDefinition();
        definition.setValue(DATATYPE, dataType);
        return tx.makePropertyKey(getName(), definition);
    }
}
