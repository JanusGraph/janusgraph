package com.thinkaurelius.titan.util.interval;

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
                return new RangeWithHoles<V>(value);
            case GREATER_THAN:
                return new Range<V>(value,null,false,true);
            case GREATER_THAN_EQUAL:
                return new Range<V>(value,null,true,true);
            case LESS_THAN:
                return new Range<V>(null,value,true,false);
            case LESS_THAN_EQUAL:
                return new Range<V>(null,value,true,true);
            default:
                throw new IllegalArgumentException("Unsupported compare type: " + compare);
        }
    }
    
}
