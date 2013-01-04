package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ArrayAdjacencyList implements AdjacencyList {

    private static final long serialVersionUID = -2868708972683152295L;

    private static final float UPDATE_FACTOR = 2.0f;

    private final AdjacencyListStrategy strategy;
    private volatile InternalRelation[] contents;


    ArrayAdjacencyList(AdjacencyListStrategy strategy) {
        this.strategy = strategy;
        contents = null;
    }

    @Override
    public synchronized AdjacencyList addEdge(InternalRelation e, ModificationStatus status) {
        Preconditions.checkNotNull(e);
        if (contents != null && contents.length >= strategy.extensionThreshold()) {
            AdjacencyList list = strategy.upgrade(this);
            list.addEdge(e, status);
            return list;
        } else {
            InternalRelation[] contents2 = new InternalRelation[(contents == null ? 0 : contents.length) + 1];
            int position = -1;
            int offset = 0;
            if (contents != null) {
                for (int i = 0; i < contents.length; i++) {
                    if (offset == 0) {
                        int compare = strategy.getComparator().compare(e, contents[i]);
                        if (compare == 0) {
                            status.nochange();
                            return this;
                        } else if (compare < 0) {
                            position = i;
                            offset = 1;
                        }
                    }
                    contents2[i + offset] = contents[i];
                }
            }
            if (position < 0) position = contents2.length - 1;
            contents2[position] = e;
            contents = contents2;
            status.change();
            return this;
        }
    }

    @Override
    public boolean containsEdge(InternalRelation e) {
        InternalRelation[] c = contents;
        return c != null && Arrays.binarySearch(c, e, strategy.getComparator()) >= 0;
    }


    @Override
    public boolean isEmpty() {
        return contents == null;
    }

    @Override
    public synchronized void removeEdge(InternalRelation e, ModificationStatus status) {
        if (contents == null) {
            status.nochange();
            return;
        }
        int position = Arrays.binarySearch(contents, e, strategy.getComparator());
        if (position >= 0) {
            status.change();
            if (contents.length == 1) {
                contents = null;
            } else {
                InternalRelation[] contents2 = new InternalRelation[contents.length - 1];
                for (int i = 0; i < contents.length; i++) {
                    if (i == position) continue;
                    contents2[i - (i > position ? 1 : 0)] = contents[i];
                }
                contents = contents2;
            }
        } else {
            status.nochange();
        }
    }

    @Override
    public AdjacencyListFactory getFactory() {
        return strategy.getFactory();
    }


    @Override
    public Iterable<InternalRelation> getEdges() {
        return new Iterable<InternalRelation>() {

            @Override
            public Iterator<InternalRelation> iterator() {
                return new InternalIterator();
            }

        };
    }

    @Override
    public Iterable<InternalRelation> getEdges(final TitanType type) {
        return new Iterable<InternalRelation>() {

            @Override
            public Iterator<InternalRelation> iterator() {
                return new InternalIterator(new TypeInternalRelation(type, true));
            }

        };
    }


    @Override
    public Iterable<InternalRelation> getEdges(final TypeGroup group) {
        return new Iterable<InternalRelation>() {

            @Override
            public Iterator<InternalRelation> iterator() {
                return new InternalIterator(new GroupInternalRelation(group, true));
            }

        };
    }


    private class InternalIterator implements Iterator<InternalRelation> {

        private InternalRelation next = null;
        private int current;
        private boolean started = false;
        private final int max;
        private final InternalRelation[] contents;


        InternalIterator() {
            contents = ArrayAdjacencyList.this.contents;
            current = -1;
            max = contents != null ? contents.length : 0;
        }

        InternalIterator(DummyInternalRelation dr) {
            this.contents = ArrayAdjacencyList.this.contents;
            if (contents == null) {
                current = -1;
                max = 0;
            } else {
                dr.makeLowerBound();
                int position = Arrays.binarySearch(contents, dr, strategy.getComparator());
                Preconditions.checkArgument(position < 0);
                this.current = -position - 2;
                Preconditions.checkArgument(current >= -1 && current < contents.length);
                dr.makeUpperBound();
                position = Arrays.binarySearch(contents, dr, strategy.getComparator());
                Preconditions.checkArgument(position < 0);
                this.max = -position - 1;
                Preconditions.checkArgument(max >= 0 && max <= contents.length);
            }
        }


        @Override
        public boolean hasNext() {
            return (current + 1 < max);
        }

        @Override
        public InternalRelation next() {
            if (!hasNext()) throw new NoSuchElementException();
            started = true;
            current++;
            return contents[current];
        }

        @Override
        public void remove() {
            if (!started) throw new NoSuchElementException();
            removeEdge(contents[current], ModificationStatus.none);
        }

    }


    @Override
    public Iterator<InternalRelation> iterator() {
        return new InternalIterator();
    }


}
