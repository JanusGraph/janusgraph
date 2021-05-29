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

import java.util.Collection;
import java.util.Map;

/**
 * A MultiVertexQuery is identical to a {@link JanusGraphVertexQuery} but executed against a set of vertices simultaneously.
 * In other words, {@link JanusGraphMultiVertexQuery} allows identical {@link JanusGraphVertexQuery} executed against a non-trivial set
 * of vertices to be executed in one batch which can significantly reduce the query latency.
 * <p>
 * The query specification methods are identical to {@link JanusGraphVertexQuery}. The result set method return Maps from the specified
 * set of anchor vertices to their respective individual result sets.
 * <p>
 * Call {@link JanusGraphTransaction#multiQuery(java.util.Collection)} to construct a multi query in the enclosing transaction.
 * <p>
 * Note, that the {@link #limit(int)} constraint applies to each individual result set.
 *
 * @see JanusGraphVertexQuery
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusGraphMultiVertexQuery<Q extends JanusGraphMultiVertexQuery<Q>> extends BaseVertexQuery<Q> {

   /* ---------------------------------------------------------------
    * Query Specification
    * ---------------------------------------------------------------
    */

    /**
     * Adds the given vertex to the set of vertices against which to execute this query.
     *
     * @param vertex
     * @return this query builder
     */
    JanusGraphMultiVertexQuery addVertex(Vertex vertex);

    /**
     * Adds the given collection of vertices to the set of vertices against which to execute this query.
     *
     * @param vertices
     * @return this query builder
     */
    JanusGraphMultiVertexQuery addAllVertices(Collection<? extends Vertex> vertices);


    @Override
    Q adjacent(Vertex vertex);

    @Override
    Q types(String... type);

    @Override
    Q types(RelationType... type);

    @Override
    Q labels(String... labels);

    @Override
    Q keys(String... keys);

    @Override
    Q direction(Direction d);

    @Override
    Q has(String type, Object value);

    @Override
    Q has(String key);

    @Override
    Q hasNot(String key);

    @Override
    Q hasNot(String key, Object value);

    @Override
    Q has(String key, JanusGraphPredicate predicate, Object value);

    @Override
    <T extends Comparable<?>> Q interval(String key, T start, T end);

    @Override
    Q limit(int limit);

    @Override
    Q orderBy(String key, Order order);

   /* ---------------------------------------------------------------
    * Query execution
    * ---------------------------------------------------------------
    */

    /**
     * Returns an iterable over all incident edges that match this query for each vertex
     *
     * @return Iterable over all incident edges that match this query for each vertex
     */
    Map<JanusGraphVertex, Iterable<JanusGraphEdge>> edges();

    /**
     * Returns an iterable over all incident properties that match this query for each vertex
     *
     * @return Iterable over all incident properties that match this query for each vertex
     */
    Map<JanusGraphVertex, Iterable<JanusGraphVertexProperty>> properties();

    /**
     * Makes a call to properties to pre-fetch the properties into the vertex cache
     */
    void preFetch();

    /**
     * Returns an iterable over all incident relations that match this query for each vertex
     *
     * @return Iterable over all incident relations that match this query for each vertex
     */
    Map<JanusGraphVertex, Iterable<JanusGraphRelation>> relations();

    /**
     * Retrieves all vertices connected to each of the query's base vertices by edges
     * matching the conditions defined in this query.
     * <p>
     *
     * @return An iterable of all vertices connected to each of the query's central vertices by matching edges
     */
    Map<JanusGraphVertex, Iterable<JanusGraphVertex>> vertices();

    /**
     * Retrieves all vertices connected to each of the query's central vertices by edges
     * matching the conditions defined in this query.
     * <p>
     * The query engine will determine the most efficient way to retrieve the vertices that match this query.
     *
     * @return A list of all vertices' ids connected to each of the query's central vertex by matching edges
     */
    Map<JanusGraphVertex, VertexList> vertexIds();

}
