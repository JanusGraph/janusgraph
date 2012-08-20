package com.thinkaurelius.titan.util.interval;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.Query;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class IntervalUtil {



    public static<V extends Comparable<V>> AtomicInterval<V> getInterval(V value, Query.Compare compare) {
        switch(compare) {
            case EQUAL:
                return new PointInterval<V>(value);
            case NOT_EQUAL: 
                if (value==null) return new Range<V>(null,null,true,true);
                return new RangeWithHoles<V>(value);
            case GREATER_THAN:
                Preconditions.checkNotNull(value,"Value cannot be null");
                return new Range<V>(value,null,false,true);
            case GREATER_THAN_EQUAL:
                Preconditions.checkNotNull(value,"Value cannot be null");
                return new Range<V>(value,null,true,true);
            case LESS_THAN:
                Preconditions.checkNotNull(value,"Value cannot be null");
                return new Range<V>(null,value,true,false);
            case LESS_THAN_EQUAL:
                Preconditions.checkNotNull(value,"Value cannot be null");
                return new Range<V>(null,value,true,true);
            default:
                throw new IllegalArgumentException("Unsupported compare type: " + compare);
        }
    }
    
}
