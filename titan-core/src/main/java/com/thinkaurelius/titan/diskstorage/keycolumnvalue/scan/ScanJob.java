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
 * A computation over edgestore entries.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ScanJob {

    /**
     * Invoked prior to any other method on this {@code ScanJob} instance.
     * Called once.
     *
     * @param config
     * @param metrics
     */
    public default void setup(Configuration config, ScanMetrics metrics) {}

    /**
     * After this method is invoked, no additional method calls on this
     * {@code ScanJob} instance are permitted.  Called once.
     *
     * @param metrics
     */
    public default void teardown(ScanMetrics metrics) {}

    /**
     * Run this {@code ScanJob}'s computation on the supplied row-key and entries.
     * <p>
     * This method may be called by concurrent threads.
     *
     * @param key
     * @param entries
     * @param metrics
     */
    public void process(StaticBuffer key, Map<SliceQuery,EntryList> entries, ScanMetrics metrics);

    /**
     * If multiple queries are returned, the first query must be a grounding query
     * @return
     */
    public List<SliceQuery> getQueries();

    /**
     * A predicate that determines whether
     * {@link #process(com.thinkaurelius.titan.diskstorage.StaticBuffer, java.util.Map, ScanMetrics)}
     * should be invoked for the given key.  If the predicate returns true,
     * then users of this interface should invoke {@code process} for the key and
     * its associated entries.  If the predicate returns false, then users of this
     * interface need not invoke {@code process} for the key and its associated entries.
     * <p>
     * This is essentially an optimization that lets implementations of this interface
     * signal to client code that a row can be safely skipped without affecting the
     * execution of this {@code ScanJob}.
     * <p>
     * This method may be called by concurrent threads.
     *
     * @return a predicate for edgestore keys
     */
    public default Predicate<StaticBuffer> getKeyFilter() {
        return b -> true; //No filter by default
    }

}
