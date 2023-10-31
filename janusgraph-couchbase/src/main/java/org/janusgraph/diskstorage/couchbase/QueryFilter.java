/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.janusgraph.diskstorage.couchbase;

public class QueryFilter {
    private String query;
    private Object[] arguments;

    public QueryFilter(String query, Object... arguments) {
        this.query = query;
        this.arguments = arguments;
    }

    public String query() {
        return query;
    }

    public Object[] arguments() {
        return arguments;
    }

    public QueryFilter combine(String operator, QueryFilter other) {
        String query = "(" + this.query + ") " + operator + "(" + other.query() + ")";
        Object[] otherArgs = other.arguments();
        Object[] newArgs = new Object[this.arguments.length + otherArgs.length];
        System.arraycopy(this.arguments, 0, newArgs, 0, this.arguments.length);
        System.arraycopy(otherArgs, 0, newArgs, this.arguments.length, otherArgs.length);
        return new QueryFilter(query, newArgs);
    }
}
