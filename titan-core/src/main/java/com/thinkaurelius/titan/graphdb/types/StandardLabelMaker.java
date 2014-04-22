package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeHandling;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;

import static com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory.HIDDEN;
import static com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory.UNIDIRECTIONAL;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardLabelMaker extends StandardTypeMaker implements LabelMaker {

    private Direction unidirectionality;

    public StandardLabelMaker(StandardTitanTx tx, IndexSerializer indexSerializer,
                              final AttributeHandling attributeHandler) {
        super(tx, indexSerializer, attributeHandler);
        unidirectionality = Direction.BOTH;
    }

    @Override
    public StandardLabelMaker directed() {
        unidirectionality = Direction.BOTH;
        return this;
    }

    @Override
    public StandardLabelMaker unidirected() {
        return unidirected(Direction.OUT);
    }

    public StandardLabelMaker unidirected(Direction dir) {
        Preconditions.checkNotNull(dir);
        unidirectionality = dir;
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
    public LabelMaker ttl(int seconds) {
        super.ttl(seconds);
        return this;
    }

    @Override
    public StandardLabelMaker hidden() {
        super.hidden();
        return this;
    }


    @Override
    public TitanLabel make() {
        TypeDefinitionMap definition = makeDefinition();
        Preconditions.checkArgument(unidirectionality==Direction.BOTH || !getMultiplicity().isUnique(unidirectionality.opposite()),
                "Unidirectional labels cannot have restricted multiplicity at the other end");
        Preconditions.checkArgument(unidirectionality==Direction.BOTH || !hasSortKey() ||
                !getMultiplicity().isUnique(unidirectionality),
                "Unidirectional labels with restricted multiplicity cannot have a sort key");
        Preconditions.checkArgument(unidirectionality!=Direction.IN || definition.getValue(HIDDEN,Boolean.class));


        definition.setValue(UNIDIRECTIONAL, unidirectionality);
        return tx.makeEdgeLabel(getName(), definition);
    }

}
