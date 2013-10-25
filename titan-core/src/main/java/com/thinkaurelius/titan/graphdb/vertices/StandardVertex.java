package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.AddedRelationsContainer;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.ConcurrentAddedRelations;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.SimpleAddedRelations;
import com.thinkaurelius.titan.util.datastructures.Retriever;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardVertex extends AbstractVertex {

    private byte lifecycle;
    private volatile AddedRelationsContainer addedRelations=AddedRelationsContainer.EMPTY;

    public StandardVertex(final StandardTitanTx tx, final long id, byte lifecycle) {
        super(tx, id);
        this.lifecycle=lifecycle;
    }

    private synchronized final void updateLifeCycle(ElementLifeCycle.Event event) {
        this.lifecycle = ElementLifeCycle.update(lifecycle,event);
    }

    @Override
    public void removeRelation(InternalRelation r) {
        if (r.isNew()) addedRelations.remove(r);
        else if (r.isLoaded()) updateLifeCycle(ElementLifeCycle.Event.REMOVED_RELATION);
        else throw new IllegalArgumentException("Unexpected relation status: " + r.isRemoved());
    }

    @Override
    public boolean addRelation(InternalRelation r) {
        Preconditions.checkArgument(r.isNew());
        if (addedRelations==AddedRelationsContainer.EMPTY) {
            if (tx().getConfiguration().isSingleThreaded()) {
                addedRelations=new SimpleAddedRelations();
            } else {
                synchronized (this) {
                    if (addedRelations==AddedRelationsContainer.EMPTY)
                        addedRelations=new ConcurrentAddedRelations();
                }
            }
        }
        if (addedRelations.add(r)) {
            updateLifeCycle(ElementLifeCycle.Event.ADDED_RELATION);
            return true;
        } else return false;
    }

    @Override
    public List<InternalRelation> getAddedRelations(Predicate<InternalRelation> query) {
        return addedRelations.getView(query);
    }

    @Override
    public Collection<Entry> loadRelations(SliceQuery query, Retriever<SliceQuery, List<Entry>> lookup) {
        return (isNew()) ? Collections.EMPTY_LIST : lookup.get(query);
    }

    @Override
    public boolean hasLoadedRelations(SliceQuery query) {
        return false;
    }

    @Override
    public boolean hasRemovedRelations() {
        return ElementLifeCycle.hasRemovedRelations(lifecycle);
    }

    @Override
    public boolean hasAddedRelations() {
        return ElementLifeCycle.hasAddedRelations(lifecycle);
    }

    @Override
    public synchronized void remove() {
        super.remove();
        updateLifeCycle(ElementLifeCycle.Event.REMOVED);
    }

    @Override
    public byte getLifeCycle() {
        return lifecycle;
    }
}
