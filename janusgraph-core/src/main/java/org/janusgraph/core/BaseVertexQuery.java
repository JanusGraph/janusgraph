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
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.query.JanusGraphPredicate;

/**
 * BaseVertexQuery constructs and executes a query over incident edges or properties from the perspective of a vertex.
 * <p>
 * A VertexQuery has some JanusGraph specific convenience methods for querying for incident edges or properties.
 * Using VertexQuery proceeds in two steps:
 * 1) Define the query by specifying what to retrieve and
 * 2) execute the query for the elements to retrieve.
 * <p>
 * This is the base interface for the specific implementations of a VertexQuery. Calling {@link org.janusgraph.core.JanusGraphVertex#query()}
 * returns a {@link JanusGraphVertexQuery} for querying a single vertex.
 * Calling {@link JanusGraphTransaction#multiQuery(java.util.Collection)} returns a {@link JanusGraphMultiVertexQuery} to execute
 * the same query against multiple vertices at the same time which is typically faster.
 *
 * @see JanusGraphVertexQuery
 * @see JanusGraphMultiVertexQuery
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */
public interface BaseVertexQuery<Q extends BaseVertexQuery<Q>> {

    /* ---------------------------------------------------------------
    * Query Specification
    * ---------------------------------------------------------------
    */

    /**
     * Restricts this query to only those edges that point to the given vertex.
     *
     * @param vertex
     * @return this query builder
     */
    Q adjacent(Vertex vertex);

    /**
     * Query for only those relations matching one of the given relation types.
     * By default, a query includes all relations in the result set.
     *
     * @param type relation types to query for
     * @return this query
     */
    Q types(String... type);

    /**
     * Query for only those relations matching one of the given relation types.
     * By default, a query includes all relations in the result set.
     *
     * @param type relation types to query for
     * @return this query
     */
    Q types(RelationType... type);

    /**
     * Query for only those edges matching one of the given edge labels.
     * By default, an edge query includes all edges in the result set.
     *
     * @param labels edge labels to query for
     * @return this query
     */
    Q labels(String... labels);

    /**
     * Query for only those properties having one of the given property keys.
     * By default, a query includes all properties in the result set.
     *
     * @param keys property keys to query for
     * @return this query
     */
    Q keys(String... keys);

    /**
     * Query only for relations in the given direction.
     * By default, both directions are queried.
     *
     * @param d Direction to query for
     * @return this query
     */
    Q direction(Direction d);

    /**
     * Query only for edges or properties that have an incident property or unidirected edge matching the given value.
     * <p>
     * If type is a property key, then the query is restricted to edges or properties having an incident property matching
     * this key-value pair.
     * If type is an edge label, then it is expected that this label is unidirected ({@link EdgeLabel#isUnidirected()}
     * and the query is restricted to edges or properties having an incident unidirectional edge pointing to the value which is
     * expected to be a {@link org.janusgraph.core.JanusGraphVertex}.
     *
     * @param type  JanusGraphType name
     * @param value Value for the property of the given key to match, or vertex to point unidirectional edge to
     * @return this query
     */
    Q has(String type, Object value);

    /**
     * Query for edges or properties that have defined property with the given key
     *
     * @param key
     * @return this query
     */
    Q has(String key);

    /**
     * Query for edges or properties that DO NOT have a defined property with the given key
     *
     * @param key
     * @return this query
     */
    Q hasNot(String key);

    /**
     * Identical to {@link #has(String, Object)} but negates the condition, i.e. matches those edges or properties
     * that DO NOT satisfy this property condition.
     *
     * @param key
     * @param value
     * @return
     */
    Q hasNot(String key, Object value);

    Q has(String key, JanusGraphPredicate predicate, Object value);

    /**
     * Query for those edges or properties that have a property for the given key
     * whose values lies in the interval by [start,end).
     *
     * @param key   property key
     * @param start value defining the start of the interval (inclusive)
     * @param end   value defining the end of the interval (exclusive)
     * @return this query
     */
    <T extends Comparable<?>> Q interval(String key, T start, T end);

    /**
     * Sets the retrieval limit for this query.
     * <p>
     * When setting a limit, executing this query will only retrieve the specified number of relations. Note, that this
     * also applies to counts.
     *
     * @param limit maximum number of relations to retrieve for this query
     * @return this query
     */
    Q limit(int limit);


    /**
     * Orders the relation results of this query according
     * to their property for the given key in the given order (increasing/decreasing).
     * <p>
     * Note, that the ordering always applies to the incident relations (edges/properties) and NOT
     * to the adjacent vertices even if only vertices are being returned.
     *
     * @param key   The key of the properties on which to order
     * @param order the ordering direction
     * @return
     */
    Q orderBy(String key, Order order);


}
