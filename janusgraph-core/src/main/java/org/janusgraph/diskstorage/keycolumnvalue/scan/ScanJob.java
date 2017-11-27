// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.keycolumnvalue.scan;

import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * A global computation over
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ScanJob extends Cloneable {

    /**
     * Invoked before a block of computation (i.e. multiple process() calls) is handed to this particular ScanJob.
     * Can be used to initialize the iteration. This method is called exactly once for each before a block of computation.
     * This method is semantically aligned with {@link org.apache.tinkerpop.gremlin.process.computer.VertexProgram#workerIterationStart(Memory)}
     *
     * This method may not be called if there is no data to be processed. Correspondingly, the end method won't be called either.
     *
     * No-op default implementation.
     *
     * @param jobConfiguration configuration for this particular job
     * @param graphConfiguration configuration options for the entire graph against which this job is executed
     * @param metrics {@link org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics} for this job
     */
    default void workerIterationStart(Configuration jobConfiguration, Configuration graphConfiguration, ScanMetrics metrics) {}

    /**
     * Invoked after a block of computation (i.e. multiple process() calls) is handed to this particular ScanJob.
     * Can be used to close any resources held by this job. This method is called exactly once for each after a block of computation.
     * This method is semantically aligned with {@link org.apache.tinkerpop.gremlin.process.computer.VertexProgram#workerIterationEnd(Memory)}
     *
     * This method may not be called if there is no data to be processed. Correspondingly, the start method won't be called either.
     *
     * No-op default implementation.
     *
     * @param metrics {@link org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics} for this job
     */
    default void workerIterationEnd(ScanMetrics metrics) {}

    /**
     * Run this {@code ScanJob}'s computation on the supplied row-key and entries.
     * <p>
     * This method will be called by a client of this interface if and only if both
     * of the following criteria are satisfied:
     * <ul>
     *     <li>
     *         The predicate returned by {@link #getKeyFilter()} must evaluate to true
     *         on the {@code key}.
     *     </li>
     *     <li>
     *         The {@code entries} parameter must contain an entry whose key is the first
     *         {@code SliceQuery} returned by {@link #getQueries()} and whose value
     *         is an {@code EntryList} with at least size one.  In other words, the
     *         initial query in this job's query list must have matched at least once.
     *     </li>
     * </ul>
     * <p>
     * Implementations may assume these two conditions are satisfied.  Calling this
     * method when either of these two conditions is false yields undefined behavior.
     *
     * <p>
     * It is the caller's responsibility to construct an {@code entries} map that
     * maps each {@code SliceQuery} to that query's matches.
     * The caller is also responsible for truncating the {@code entries} values
     * to honor {@link SliceQuery#getLimit()} when {@link SliceQuery#hasLimit()}
     * is true.  Passing in an {@code entries} value longer than the limit set in
     * its respective key yields undefined behavior.
     * <p>
     * This method may be called by concurrent threads in a single process.
     *
     * @param key
     * @param entries
     * @param metrics
     */
    void process(StaticBuffer key, Map<SliceQuery, EntryList> entries, ScanMetrics metrics);

    /**
     * Returns one or more {@code SliceQuery} instances belonging to this {@code ScanJob}.
     * <p>
     * Before calling
     * {@link #process(org.janusgraph.diskstorage.StaticBuffer, java.util.Map, ScanMetrics)},
     * users of this interface must check that the key in question contains at least one
     * entry matching the initial {@code SliceQuery} returned by this method.  See the javadoc
     * for the {@code process} method for more information.
     * <p>
     * If this method returns more than one query, then the initial query's lower bound must
     * be all zero bits and the initial query's upper bound must be all one bits (per the
     * preconditions in {@code StandardScannerExecutor}, the reference {@code ScanJob} executor).
     *
     * @return one or more queries
     */
    List<SliceQuery> getQueries();

    /**
     * A predicate that determines whether
     * {@link #process(org.janusgraph.diskstorage.StaticBuffer, java.util.Map, ScanMetrics)}
     * should be invoked for the given key.  If the predicate returns true,
     * then users of this interface should invoke {@code process} for the key and
     * its associated entries.  If the predicate returns false, then users of this
     * interface need not invoke {@code process} for the key and its associated entries.
     * <p>
     * This is essentially an optimization that lets implementations of this interface
     * signal to client code that a row can be safely skipped without affecting the
     * execution of this {@code ScanJob}.
     * <p>
     * The returned predicate may be called by concurrent threads in a single process.
     *
     * @return a threadsafe predicate for edgestore keys
     */
    default Predicate<StaticBuffer> getKeyFilter() {
        return b -> true; //No filter by default
    }

    /**
     * Returns a clone of this ScanJob. The clone will not yet be initialized for computation but all of
     * its internal state (if any) must match that of the original copy.
     *
     * @return A clone of this {@link org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob}
     */
    ScanJob clone();

}
