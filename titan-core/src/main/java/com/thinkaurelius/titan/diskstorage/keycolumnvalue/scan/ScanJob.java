package com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ScanJob {

    public default void setup(Configuration config, ScanMetrics metrics) {}

    public default void teardown(ScanMetrics metrics) {}

    public void process(StaticBuffer key, Map<SliceQuery,EntryList> entries, ScanMetrics metrics);

    /**
     * If multiple queries are returned, the first query must be a grounding query
     * @return
     */
    public List<SliceQuery> getQueries();

    public default Predicate<StaticBuffer> getKeyFilter() {
        return b -> true; //No filter by default
    }

}
