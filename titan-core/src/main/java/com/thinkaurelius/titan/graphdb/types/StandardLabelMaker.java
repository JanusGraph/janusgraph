package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeHandling;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;

import static com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory.INVERTED_DIRECTION;
import static com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory.UNIDIRECTIONAL;
import static com.tinkerpop.blueprints.Direction.IN;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardLabelMaker extends StandardTypeMaker implements LabelMaker {

    private boolean isUnidirectional;
    private boolean invertedBaseDirection;

    public StandardLabelMaker(StandardTitanTx tx, IndexSerializer indexSerializer,
                              final AttributeHandling attributeHandler) {
        super(tx, indexSerializer, attributeHandler);
        isUnidirectional = false;
        invertedBaseDirection = false;
    }

    @Override
    public StandardLabelMaker directed() {
        isUnidirectional = false;
        return this;
    }

    @Override
    public StandardLabelMaker unidirected() {
        isUnidirectional = true;
        return this;
    }

    public StandardLabelMaker invertDirection() {
        invertedBaseDirection = true;
        return this;
    }

    @Override
    public StandardLabelMaker multiplicity(Multiplicity multiplicity) {
        super.multiplicity(multiplicity);
        return this;
    }

    @Override
    public StandardLabelMaker signature(TitanType... types) {
        super.signature(types);
        return this;
    }

    @Override
    public StandardLabelMaker sortKey(TitanType... types) {
        super.sortKey(types);
        return this;
    }

    @Override
    public StandardLabelMaker sortOrder(Order order) {
        super.sortOrder(order);
        return this;
    }

    @Override
    public StandardLabelMaker hidden() {
        super.hidden();
        return this;
    }


    @Override
    public TitanLabel make() {
        Preconditions.checkArgument(!isUnidirectional ||
                !(getMultiplicity()==Multiplicity.ONE2MANY || getMultiplicity()==Multiplicity.ONE2ONE),
                "Unidirectional labels cannot have restricted multiplicity at the end vertex");
        Preconditions.checkArgument(!(isUnidirectional && hasSortKey()) ||
                !(getMultiplicity()==Multiplicity.MANY2ONE),
                "Unidirectional labels with restricted multiplicity cannot have a sort key");
        Preconditions.checkArgument(!invertedBaseDirection || isUnidirectional);


        TypeDefinitionMap definition = makeDefinition();
        definition.setValue(UNIDIRECTIONAL, isUnidirectional);
        definition.setValue(INVERTED_DIRECTION,invertedBaseDirection);
        return tx.makeEdgeLabel(getName(), definition);
    }

}
