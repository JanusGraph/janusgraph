package com.thinkaurelius.titan.graphdb.database.management;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TitanTypeIndex;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.Token;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanTypeVertex;
import com.tinkerpop.blueprints.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanTypeIndexWrapper implements TitanTypeIndex {

    private final InternalType type;

    public TitanTypeIndexWrapper(InternalType type) {
        Preconditions.checkArgument(type!=null && type.getBaseType()!=null);
        this.type = type;
    }

    @Override
    public TitanType getType() {
        return type.getBaseType();
    }

    @Override
    public String getName() {
        String[] comps = Token.splitSeparatedName(type.getName());
        assert comps.length==3;
        return comps[2];
    }

    @Override
    public Order getSortOrder() {
        return type.getSortOrder();

    }

    @Override
    public TitanType[] getSortKey() {
        StandardTitanTx tx = type.tx();
        long[] ids = type.getSortKey();
        TitanType[] keys = new TitanType[ids.length];
        for (int i = 0; i < keys.length; i++) {
            keys[i]=tx.getExistingType(ids[i]);
        }
        return keys;
    }

    @Override
    public Direction getDirection() {
        if (type.isUnidirected(Direction.BOTH)) return Direction.BOTH;
        else if (type.isUnidirected(Direction.OUT)) return Direction.OUT;
        else if (type.isUnidirected(Direction.IN)) return Direction.IN;
        throw new AssertionError();
    }

}
