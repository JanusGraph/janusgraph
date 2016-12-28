package org.janusgraph.graphdb.transaction.addedrelations;

import com.google.common.base.Predicate;
import org.janusgraph.graphdb.internal.InternalRelation;

import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ConcurrentAddedRelations extends SimpleAddedRelations {

    @Override
    public synchronized boolean add(InternalRelation relation) {
        return super.add(relation);
    }

    @Override
    public synchronized boolean remove(InternalRelation relation) {
        return super.remove(relation);
    }

    @Override
    public synchronized boolean isEmpty() {
        return super.isEmpty();
    }

    @Override
    public synchronized List<InternalRelation> getView(Predicate<InternalRelation> filter) {
        return super.getView(filter);
    }

}
