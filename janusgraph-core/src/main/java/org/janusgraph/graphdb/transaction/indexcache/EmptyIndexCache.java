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

package org.janusgraph.graphdb.transaction.indexcache;

import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class EmptyIndexCache implements IndexCache {

    private static final EmptyIndexCache INSTANCE = new EmptyIndexCache();

    private static final Logger log = LoggerFactory.getLogger(EmptyIndexCache.class);

    private EmptyIndexCache() {
    }

    public static EmptyIndexCache getInstance() {
        return INSTANCE;
    }

    private void logWarning() {
        log.warn("Index cache is already closed");
    }

    @Override
    public void add(JanusGraphVertexProperty property) {
        logWarning();
    }

    @Override
    public void remove(JanusGraphVertexProperty property) {
        logWarning();
    }

    @Override
    public Iterable<JanusGraphVertexProperty> get(final Object value, final PropertyKey key) {
        logWarning();
        return Collections.emptyList();
    }

    @Override
    public void close() {
    }
}
