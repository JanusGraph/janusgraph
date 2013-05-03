package com.thinkaurelius.titan.graphdb.transaction.addedrelations;

import com.google.common.base.Predicate;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleAddedRelations extends ArrayList<InternalRelation> implements AddedRelationsContainer {

    private static final int INITIAL_ADDED_SIZE = 10;

    public SimpleAddedRelations() {
        super(INITIAL_ADDED_SIZE);
    }

    @Override
    public boolean add(InternalRelation relation) {
        return super.add(relation);
    }

    @Override
    public boolean remove(InternalRelation relation) {
        return super.remove(relation);
    }

    @Override
    public List<InternalRelation> getView(Predicate<InternalRelation> filter) {
        List<InternalRelation> result = new ArrayList<InternalRelation>();
        for (InternalRelation r : this) {
            if (filter.apply(r)) result.add(r);
        }
        return result;
    }

    @Override
    public Collection<InternalRelation> getAll() {
        return this;
    }
}
