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

package org.janusgraph.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.graphdb.query.BaseQuery;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RawQuery extends BaseQuery {

    private final String store;
    private final String query;
    private final Parameter[] parameters;
    private ImmutableList<IndexQuery.OrderEntry> orders;
    private int offset;

    public RawQuery(String store, String query, Parameter[] parameters) {
        this(store, query, ImmutableList.of(), parameters);
    }

    public RawQuery(String store, String query, ImmutableList<IndexQuery.OrderEntry> orders, Parameter[] parameters) {
        Preconditions.checkNotNull(store);
        Preconditions.checkArgument(StringUtils.isNotBlank(query));
        Preconditions.checkNotNull(orders);
        Preconditions.checkNotNull(parameters);
        this.store = store;
        this.query = query;
        this.parameters = parameters;
        this.offset = 0;
        this.orders = orders;
    }

    public RawQuery setOffset(int offset) {
        Preconditions.checkArgument(offset>=0,"Invalid offset: %s",offset);
        this.offset=offset;
        return this;
    }

    @Override
    public RawQuery setLimit(int limit) {
        super.setLimit(limit);
        return this;
    }

    public int getOffset() {
        return offset;
    }

    public String getStore() {
        return store;
    }

    public String getQuery() {
        return query;
    }

    public ImmutableList<IndexQuery.OrderEntry> getOrders() {
        return orders;
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
