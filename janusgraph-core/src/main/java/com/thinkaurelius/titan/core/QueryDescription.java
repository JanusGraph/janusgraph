package com.thinkaurelius.titan.core;


import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface QueryDescription {

    /**
     * Returns a string representation of the entire query
     * @return
     */
    @Override
    public String toString();

    /**
     * Returns how many individual queries are combined into this query, meaning, how many
     * queries will be executed in one batch.
     *
     * @return
     */
    public int getNoCombinedQueries();

    /**
     * Returns the number of sub-queries this query is comprised of. Each sub-query represents one OR clause, i.e.,
     * the union of each sub-query's result is the overall result.
     *
     * @return
     */
    public int getNoSubQueries();

    /**
     * Returns a list of all sub-queries that comprise this query
     * @return
     */
    public List<? extends SubQuery> getSubQueries();

    /**
     * Represents one sub-query of this query. Each sub-query represents one OR clause.
     */
    public interface SubQuery {

        /**
         * Whether this query is fitted, i.e. whether the returned results must be filtered in-memory.
         * @return
         */
        public boolean isFitted();

        /**
         * Whether this query respects the sort order of parent query or requires sorting in-memory.
         * @return
         */
        public boolean isSorted();

    }


}
