package org.janusgraph.core;

import org.janusgraph.core.schema.Parameter;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * A GraphQuery that queries for graph elements directly against a particular indexing backend and hence allows this
 * query mechanism to exploit the full range of features and functionality of the indexing backend.
 * However, the results returned by this query will not be adjusted to the modifications in a transaction. If there
 * are no changes in a transaction, this won't matter. If there are, the results of this query may not be consistent
 * with the transactional state.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusIndexQuery {

    /**
     * Specifies the maxium number of elements to return
     *
     * @param limit
     * @return
     */
    public JanusIndexQuery limit(int limit);

    /**
     * Specifies the offset of the query. Query results will be retrieved starting at the given offset.
     * @param offset
     * @return
     */
    public JanusIndexQuery offset(int offset);

    /**
     * Adds the given parameter to the list of parameters of this query.
     * Parameters are passed right through to the indexing backend to modify the query behavior.
     * @param para
     * @return
     */
    public JanusIndexQuery addParameter(Parameter para);

    /**
     * Adds the given parameters to the list of parameters of this query.
     * Parameters are passed right through to the indexing backend to modify the query behavior.
     * @param paras
     * @return
     */
    public JanusIndexQuery addParameters(Iterable<Parameter> paras);

    /**
     * Adds the given parameters to the list of parameters of this query.
     * Parameters are passed right through to the indexing backend to modify the query behavior.
     * @param paras
     * @return
     */
    public JanusIndexQuery addParameters(Parameter... paras);

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
    public JanusIndexQuery setElementIdentifier(String identifier);

    /**
     * Returns all vertices that match the query in the indexing backend.
     *
     * @return
     */
    public Iterable<Result<JanusVertex>> vertices();

    /**
     * Returns all edges that match the query in the indexing backend.
     *
     * @return
     */
    public Iterable<Result<JanusEdge>> edges();

    /**
     * Returns all properties that match the query in the indexing backend.
     *
     * @return
     */
    public Iterable<Result<JanusVertexProperty>> properties();

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
