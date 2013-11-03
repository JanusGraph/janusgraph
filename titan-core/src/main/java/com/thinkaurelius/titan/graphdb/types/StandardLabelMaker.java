package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;

import static com.thinkaurelius.titan.graphdb.types.TypeAttributeType.UNIDIRECTIONAL;
import static com.tinkerpop.blueprints.Direction.IN;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardLabelMaker extends StandardTypeMaker implements LabelMaker {

    private boolean isUnidirectional;

    public StandardLabelMaker(StandardTitanTx tx, IndexSerializer indexSerializer) {
        super(tx, indexSerializer);
        isUnidirectional = false;

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

    @Override
    public LabelMaker oneToMany(UniquenessConsistency consistency) {
        super.unique(Direction.IN, consistency);
        return this;
    }

    @Override
    public LabelMaker oneToMany() {
        return oneToMany(UniquenessConsistency.LOCK);
    }

    @Override
    public LabelMaker manyToOne(UniquenessConsistency consistency) {
        super.unique(Direction.OUT, consistency);
        return this;
    }

    @Override
    public LabelMaker manyToOne() {
        return manyToOne(UniquenessConsistency.LOCK);
    }

    @Override
    public LabelMaker oneToOne(UniquenessConsistency consistency) {
        super.unique(Direction.BOTH, consistency);
        return this;
    }

    @Override
    public LabelMaker oneToOne() {
        return oneToOne(UniquenessConsistency.LOCK);
    }

    @Override
    public LabelMaker manyToMany() {
        super.unique(Direction.BOTH, null);
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
    public StandardLabelMaker unModifiable() {
        super.unModifiable();
        return this;
    }

    @Override
    public StandardLabelMaker makeStatic(Direction direction) {
        super.makeStatic(direction);
        return this;
    }

    @Override
    public TitanLabel make() {
        Preconditions.checkArgument(!isUnidirectional ||
                (!isUnique(IN) && !isStatic(IN)),
                "Unidirectional labels cannot be unique or static");

        TypeAttribute.Map definition = makeDefinition();
        definition.setValue(UNIDIRECTIONAL, isUnidirectional);
        return tx.makeEdgeLabel(getName(), definition);
    }

}
