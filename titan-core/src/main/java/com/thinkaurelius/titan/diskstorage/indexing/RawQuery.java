package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Parameter;
import com.thinkaurelius.titan.graphdb.query.BaseQuery;
import org.apache.commons.lang.StringUtils;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RawQuery extends BaseQuery {

    private final String store;
    private final String query;
    private final Parameter[] parameters;


    public RawQuery(String store, String query, Parameter[] parameters) {
        Preconditions.checkNotNull(store);
        Preconditions.checkArgument(StringUtils.isNotBlank(query));
        Preconditions.checkNotNull(parameters);
        this.store = store;
        this.query = query;
        this.parameters = parameters;
    }

    public String getStore() {
        return store;
    }

    public String getQuery() {
        return query;
    }

    public Parameter[] getParameters() {
        return parameters;
    }

    public static class Result<O> {

        private final O result;
        private final double score;


        public Result(O result, double score) {
            this.result = result;
            this.score = score;
        }

        public O getResult() {
            return result;
        }

        public double getScore() {
            return score;
        }
    }

}
