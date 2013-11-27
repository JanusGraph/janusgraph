package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanIndexQuery {

    /**
     * Specifies the maxium number of elements to return
     *
     * @param limit
     * @return
     */
    public TitanIndexQuery limit(int limit);

    /**
     * Adds the given parameter to the list of parameters of this query.
     * Parameters are passed right through to the indexing backend to modify the query behavior.
     * @param para
     * @return
     */
    public TitanIndexQuery addParameter(Parameter para);

    /**
     * Adds the given parameters to the list of parameters of this query.
     * Parameters are passed right through to the indexing backend to modify the query behavior.
     * @param paras
     * @return
     */
    public TitanIndexQuery addParameters(Iterable<Parameter> paras);

    /**
     * Adds the given parameters to the list of parameters of this query.
     * Parameters are passed right through to the indexing backend to modify the query behavior.
     * @param paras
     * @return
     */
    public TitanIndexQuery addParameters(Parameter... paras);

    /**
     * Returns all vertices that match the query in the indexing backend.
     *
     * @return
     */
    public Iterable<Result<Vertex>> vertices();

    /**
     * Returns all edges that match the query in the indexing backend.
     *
     * @return
     */
    public Iterable<Result<Edge>> edges();

    /**
     * Container of a query result with its score.
     * @param <V>
     */
    public interface Result<V extends Element> {

        /**
         * Returns the element that matches the query
         *
         * @return
         */
        public V getElement();

        /**
         * Returns the score of the result with respect to the query (if available)
         * @return
         */
        public double getScore();

    }


}
