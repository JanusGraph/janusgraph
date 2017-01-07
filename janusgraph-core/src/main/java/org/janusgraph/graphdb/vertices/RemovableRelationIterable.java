package org.janusgraph.graphdb.vertices;

import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.graphdb.internal.InternalRelation;

import java.util.Iterator;

public class RemovableRelationIterable<O extends JanusGraphRelation>
        implements Iterable<O> {

    private final Iterable<InternalRelation> iterable;

    public RemovableRelationIterable(Iterable<InternalRelation> iter) {
        iterable = iter;
    }

    @Override
    public Iterator<O> iterator() {
        return new RemovableRelationIterator<O>(iterable.iterator());
    }

}
