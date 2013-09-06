package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.*;

import java.util.Collection;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface TitanMultiVertexQuery extends BaseVertexQuery {

   /* ---------------------------------------------------------------
    * Query Specification
    * ---------------------------------------------------------------
    */

    public TitanMultiVertexQuery addVertex(TitanVertex vertex);

    public TitanMultiVertexQuery addAllVertices(Collection<TitanVertex> vertices);

    @Override
    public TitanMultiVertexQuery labels(String... labels);

    @Override
    public TitanMultiVertexQuery types(TitanType... type);

    @Override
    public TitanMultiVertexQuery direction(Direction d);

    @Override
    public TitanMultiVertexQuery has(String key);

    @Override
    public TitanMultiVertexQuery hasNot(String key);

    @Override
    public TitanMultiVertexQuery has(String type, Object value);

    @Override
    public TitanMultiVertexQuery hasNot(String key, Object value);

    @Override
    public TitanMultiVertexQuery has(String key, Predicate predicate, Object value);

    @Override
    public <T extends Comparable<?>> TitanMultiVertexQuery interval(String key, T start, T end);

    @Override
    public TitanMultiVertexQuery limit(int limit);


   /* ---------------------------------------------------------------
    * Query execution
    * ---------------------------------------------------------------
    */


    /**
     * Returns an iterable over all incident edges that match this query
     *
     * @return Iterable over all incident edges that match this query
     */
    public Map<TitanVertex,Iterable<TitanEdge>> titanEdges();

    /**
     * Returns an iterable over all incident properties that match this query
     *
     * @return Iterable over all incident properties that match this query
     */
    public Map<TitanVertex,Iterable<TitanProperty>> properties();


    /**
     * Returns an iterable over all incident relations that match this query
     *
     * @return Iterable over all incident relations that match this query
     */
    public Map<TitanVertex,Iterable<TitanRelation>> relations();

    /**
     * Retrieves all vertices connected to this query's central vertex by edges
     * matching the conditions defined in this query.
     * <p/>
     * No guarantee is made as to the order in which the vertices are listed. Use {@link com.thinkaurelius.titan.core.VertexList#sort()}
     * to sort by vertex idAuthorities most efficiently.
     * <p/>
     * The query engine will determine the most efficient way to retrieve the vertices that match this query.
     *
     * @return A list of all vertices connected to this query's central vertex by matching edges
     */
    public Map<TitanVertex,Iterable<TitanVertex>> vertices();

    public Map<TitanVertex,VertexList> vertexIds();

}
