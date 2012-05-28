package com.thinkaurelius.titan.util.interval;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class RangeWithHoles<V extends Comparable<V>> extends Range<V> {
    
    private final ImmutableSet<V> holes;

    public RangeWithHoles(V hole) {
        this(ImmutableSet.of(hole));
    }

    public RangeWithHoles(ImmutableSet<V> holes) {
        this(null,null,holes);

    }

    public RangeWithHoles(V start, V end, ImmutableSet<V> holes) {
        super(start, end);
        checkHoles(holes);
        this.holes = holes;
    }

    public RangeWithHoles(V start, V end, boolean startInclusive, boolean endInclusive, ImmutableSet<V> holes) {
        super(start, end, startInclusive, endInclusive);
        checkHoles(holes);
        this.holes = holes;
    }
    
    private void checkHoles(ImmutableSet<V> holes) {
        Preconditions.checkNotNull(holes);
        Preconditions.checkArgument(!holes.isEmpty(), "Need to specify non-empty set of holes");
        for (V hole : holes) {
            Preconditions.checkNotNull(hole,"A hole may not be null");
            Preconditions.checkArgument(super.inInterval(hole),"Hole must lie in interval");
        }
    }


    @Override
    public boolean hasHoles() {
        return true;
    }

    @Override
    public Set<V> getHoles() {
        return holes;
    }

    @Override
    public boolean inInterval(Object obj) {
        return super.inInterval(obj) && !holes.contains(obj);
    }

    @Override
    public AtomicInterval<V> intersect(AtomicInterval<V> other) {
        AtomicInterval<V> res = super.intersect(other);
        if (res==null) return null;
        HashSet<V> newholes = new HashSet<V>();
        newholes.addAll(holes);
        if (other.hasHoles()) newholes.addAll(other.getHoles());
        Iterator<V> iter = newholes.iterator();
        while (iter.hasNext()) {
            if (!res.inInterval(iter.next())) iter.remove();
        }
        if (newholes.isEmpty()) {
            return res;
        } else if (res.isPoint()) {
            assert newholes.contains(res.getStartPoint()); //The only point is also a hole => empty interval
            return null;
        } else {
            return new RangeWithHoles(res.getStartPoint(),res.getEndPoint(),res.startInclusive(),res.endInclusive(),ImmutableSet.of(newholes));
        }
    }
    
}
