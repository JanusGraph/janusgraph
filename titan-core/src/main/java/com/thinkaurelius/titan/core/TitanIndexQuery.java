package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.core.schema.Parameter;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

/**
 * A GraphQuery that queries for graph elements directly against a particular indexing backend and hence allows this
 * query mechanism to exploit the full range of features and functionality of the indexing backend.
 * However, the results returned by this query will not be adjusted to the modifications in a transaction. If there
 * are no changes in a transaction, this won't matter. If there are, the results of this query may not be consistent
 * with the transactional state.
 *
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
     * Specifies the offset of the query. Query results will be retrieved starting at the given offset.
     * @param offset
     * @return
     */
    public TitanIndexQuery offset(int offset);

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
     * Sets the element identifier string that is used by this query builder as the token to identifier key references
     * in the query string.
     * <p/>
     * For example, in the query 'v.name: Tom' the element identifier is 'v.'
     *
     *
     * @param identifier The element identifier which must not be blank
     * @return This query builder
     */
    public TitanIndexQuery setElementIdentifier(String identifier);

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
     * Returns all properties that match the query in the indexing backend.
     *
     * @return
     */
    public Iterable<Result<TitanProperty>> properties();

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
