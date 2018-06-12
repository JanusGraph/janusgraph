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
 * A BackendQuery is a query that can be updated to a new limit.
 * <p>
 * This is useful in query execution where the query limit is successively relaxed to find all the needed elements
 * of the result set.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface BackendQuery<Q extends BackendQuery> extends Query {

    /**
     * Creates a new query identical to the current one but with the specified limit.
     *
     * @param newLimit
     * @return
     */
    Q updateLimit(int newLimit);

}
