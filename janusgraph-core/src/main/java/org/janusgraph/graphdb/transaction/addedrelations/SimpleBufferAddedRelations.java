package com.thinkaurelius.titan.graphdb.transaction.addedrelations;

import com.google.common.base.Predicate;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;

import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleBufferAddedRelations implements AddedRelationsContainer {

    private static final int INITIAL_ADDED_SIZE = 10;
    private static final int INITIAL_DELETED_SIZE = 10;
    private static final int MAX_DELETED_SIZE = 500;

    private List<InternalRelation> added;
    private List<InternalRelation> deleted;

    public SimpleBufferAddedRelations() {
        added = new ArrayList<InternalRelation>(INITIAL_ADDED_SIZE);
        deleted = null;
    }

    @Override
    public boolean add(InternalRelation relation) {
        return added.add(relation);
    }

    @Override
    public boolean remove(InternalRelation relation) {
        if (added.isEmpty()) return false;
        if (deleted==null) deleted = new ArrayList<InternalRelation>(INITIAL_DELETED_SIZE);
        boolean del = deleted.add(relation);
        if (deleted.size()>MAX_DELETED_SIZE) cleanup();
        return del;
    }

    @Override
    public boolean isEmpty() {
        cleanup();
        return added.isEmpty();
    }

    private void cleanup() {
        if (deleted==null || deleted.isEmpty()) return;
        Set<InternalRelation> deletedSet = new HashSet<InternalRelation>(deleted);
        deleted=null;
        List<InternalRelation> newadded = new ArrayList<InternalRelation>(added.size()-deletedSet.size()/2);
        for (InternalRelation r : added) {
            if (!deletedSet.contains(r)) newadded.add(r);
        }
        added=newadded;
    }

    @Override
    public List<InternalRelation> getView(Predicate<InternalRelation> filter) {
        cleanup();
        List<InternalRelation> result = new ArrayList<InternalRelation>();
        for (InternalRelation r : added) {
            if (filter.apply(r)) result.add(r);
        }
        return result;
    }

    @Override
    public Collection<InternalRelation> getAll() {
        cleanup();
        return added;
    }
}
