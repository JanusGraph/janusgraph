// Copyright 2022 JanusGraph Authors
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

package org.janusgraph.graphdb.transaction.subquerycache;

import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

public class EmptySubqueryCache implements SubqueryCache {

    private static final EmptySubqueryCache INSTANCE = new EmptySubqueryCache();

    private static final Logger log = LoggerFactory.getLogger(EmptySubqueryCache.class);

    private EmptySubqueryCache() {
    }

    public static EmptySubqueryCache getInstance() {
        return INSTANCE;
    }

    private void logWarning() {
        log.warn("Subquery cache is already closed");
    }

    @Override
    public List<Object> getIfPresent(JointIndexQuery.Subquery query) {
        logWarning();
        return Collections.emptyList();
    }

    @Override
    public void put(JointIndexQuery.Subquery query, List<Object> values) {
        logWarning();
    }

    @Override
    public List<Object> get(JointIndexQuery.Subquery query, Callable<? extends List<Object>> valueLoader) throws Exception {
        logWarning();
        return Collections.emptyList();
    }

    @Override
    public void close() {
    }
}
