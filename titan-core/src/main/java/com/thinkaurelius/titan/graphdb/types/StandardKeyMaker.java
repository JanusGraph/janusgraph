package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeHandling;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;

import java.lang.reflect.Modifier;

import static com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory.DATATYPE;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardKeyMaker extends StandardTypeMaker implements KeyMaker {

    private Class<?> dataType;

    public StandardKeyMaker(StandardTitanTx tx, IndexSerializer indexSerializer,
                            final AttributeHandling attributeHandler) {
        super(tx, indexSerializer, attributeHandler);
        dataType = null;
        cardinality(Cardinality.SINGLE);
    }

    @Override
    public StandardKeyMaker dataType(Class<?> clazz) {
        Preconditions.checkArgument(clazz != null, "Need to specify a data type");
        dataType = clazz;
        return this;
    }

    @Override
    public StandardKeyMaker cardinality(Cardinality cardinality) {
        super.multiplicity(Multiplicity.convert(cardinality));
        return this;
    }


    @Override
    public StandardKeyMaker hidden() {
        super.hidden();
        return this;
    }

    @Override
    public StandardKeyMaker signature(TitanType... types) {
        super.signature(types);
        return this;
    }

    @Override
    public StandardKeyMaker sortKey(TitanType... types) {
        super.sortKey(types);
        return this;
    }

    @Override
    public StandardKeyMaker sortOrder(Order order) {
        super.sortOrder(order);
        return this;
    }

    @Override
    public TitanKey make() {
        Preconditions.checkArgument(dataType != null, "Need to specify a datatype");
        Preconditions.checkArgument(!dataType.isPrimitive(), "Primitive types are not supported. Use the corresponding object type, e.g. Integer.class instead of int.class [%s]", dataType);
        Preconditions.checkArgument(!dataType.isInterface(), "Datatype must be a class and not an interface: %s", dataType);
        Preconditions.checkArgument(dataType.isArray() || !Modifier.isAbstract(dataType.getModifiers()), "Datatype cannot be an abstract class: %s", dataType);

        TypeDefinitionMap definition = makeDefinition();
        definition.setValue(DATATYPE, dataType);
        return tx.makePropertyKey(getName(), definition);
    }
}
