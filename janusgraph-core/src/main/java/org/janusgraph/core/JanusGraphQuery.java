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

package org.janusgraph.core;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.janusgraph.graphdb.query.JanusGraphPredicate;

import java.util.Collection;

/**
 * Constructs a query against a mixed index to retrieve all elements (either vertices or edges)
 * that match all conditions.
 * <p>
 * Finding matching elements efficiently using this query mechanism requires that appropriate index structures have
 * been defined for the keys. See {@link org.janusgraph.core.schema.JanusGraphManagement} for more information
 * on how to define index structures in JanusGraph.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @since 0.3.0
 */

public interface JanusGraphQuery<Q extends JanusGraphQuery<Q>> {

   /* ---------------------------------------------------------------
    * Query Specification
    * ---------------------------------------------------------------
    */

    /**
     * The returned element must have a property for the given key that matches the condition according to the
     * specified relation
     *
     * @param key       Key that identifies the property
     * @param predicate Relation between property and condition
     * @param condition
     * @return This query
     */
    Q has(String key, JanusGraphPredicate predicate, Object condition);

    Q has(String key);

    Q hasNot(String key);

    Q has(String key, Object value);

    Q hasNot(String key, Object value);

    Q or(Collection<Q> subQueries);

    <T extends Comparable<?>> Q interval(String key, T startValue, T endValue);

    /**
     * Limits the size of the returned result set
     *
     * @param max The maximum number of results to return
     * @return This query
     */
    Q limit(final int max);

    /**
     * Orders the element results of this query according
     * to their property for the given key in the given order (increasing/decreasing).
     *
     * @param key   The key of the properties on which to order
     * @param order the ordering direction
     * @return
     */
    Q orderBy(String key, Order order);


    /* ---------------------------------------------------------------
    * Query Execution
    * ---------------------------------------------------------------
    */

    /**
     * Returns all vertices that match the conditions.
     *
     * @return
     */
    Iterable<JanusGraphVertex> vertices();

    /**
     * Returns all edges that match the conditions.
     *
     * @return
     */
    Iterable<JanusGraphEdge> edges();

    /**
     * Returns all properties that match the conditions
     *
     * @return
     */
    Iterable<JanusGraphVertexProperty> properties();


}
