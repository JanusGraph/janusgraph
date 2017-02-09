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

package org.janusgraph.graphdb.query;

/**
 * Standard implementation of {@link Query}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class BaseQuery implements Query {

    private int limit;

    public BaseQuery() {
        this(NO_LIMIT);
    }

    public BaseQuery(final int limit) {
        assert limit >= 0;
        this.limit = limit;
    }

    /**
     * Sets the limit of the query if it wasn't specified in the constructor
     * @param limit
     * @return
     */
    public BaseQuery setLimit(final int limit) {
        assert limit >= 0;
        this.limit = limit;
        return this;
    }

    @Override
    public int getLimit() {
        return limit;
    }

    @Override
    public boolean hasLimit() {
        return limit != Query.NO_LIMIT;
    }

}
