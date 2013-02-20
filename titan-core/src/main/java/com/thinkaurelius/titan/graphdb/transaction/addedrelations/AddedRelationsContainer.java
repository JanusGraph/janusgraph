package com.thinkaurelius.titan.graphdb.transaction.addedrelations;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;

import java.util.List;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface AddedRelationsContainer {

    public boolean add(InternalRelation relation);

    public boolean remove(InternalRelation relation);

    public List<InternalRelation> getView(Predicate<InternalRelation> filter);

    public boolean isEmpty();

    /**
     * This method returns all relations in this container. It may only be invoked at the end
     * of the transaction after there are no additional changes. Otherwise the behavior is non deterministic.
     * @return
     */
    public Iterable<InternalRelation> getAll();


    public static final AddedRelationsContainer EMPTY = new AddedRelationsContainer() {
        @Override
        public boolean add(InternalRelation relation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(InternalRelation relation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<InternalRelation> getView(Predicate<InternalRelation> filter) {
            return ImmutableList.of();
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public Iterable<InternalRelation> getAll() {
            return ImmutableList.of();
        }
    };

}
