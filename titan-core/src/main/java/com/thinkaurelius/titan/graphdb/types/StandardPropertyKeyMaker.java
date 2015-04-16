package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeHandler;
import com.thinkaurelius.titan.graphdb.internal.Order;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;

import java.lang.reflect.Modifier;

import static com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory.DATATYPE;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardPropertyKeyMaker extends StandardRelationTypeMaker implements PropertyKeyMaker {

    private Class<?> dataType;

    public StandardPropertyKeyMaker(StandardTitanTx tx, String name, IndexSerializer indexSerializer,
                                    final AttributeHandler attributeHandler) {
        super(tx, name, indexSerializer, attributeHandler);
        dataType = null;
        cardinality(Cardinality.SINGLE);
    }

    @Override
    TitanSchemaCategory getSchemaCategory() {
        return TitanSchemaCategory.PROPERTYKEY;
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
        return this;
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
        Preconditions.checkArgument(!dataType.isPrimitive(), "Primitive types are not supported. Use the corresponding object type, e.g. Integer.class instead of int.class [%s]", dataType);
        Preconditions.checkArgument(!dataType.isInterface(), "Datatype must be a class and not an interface: %s", dataType);
        Preconditions.checkArgument(dataType.isArray() || !Modifier.isAbstract(dataType.getModifiers()), "Datatype cannot be an abstract class: %s", dataType);

        TypeDefinitionMap definition = makeDefinition();
        definition.setValue(DATATYPE, dataType);
        return tx.makePropertyKey(getName(), definition);
    }
}
