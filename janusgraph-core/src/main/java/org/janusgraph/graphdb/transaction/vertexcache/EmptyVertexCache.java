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

package org.janusgraph.graphdb.transaction.vertexcache;

import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.util.datastructures.Retriever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class EmptyVertexCache implements VertexCache {

    private static final EmptyVertexCache INSTANCE = new EmptyVertexCache();

    private static final Logger log = LoggerFactory.getLogger(EmptyVertexCache.class);

    private EmptyVertexCache() {
    }

    public static EmptyVertexCache getInstance() {
        return INSTANCE;
    }

    private void logWarning() {
        log.warn("Vertex cache is already closed");
    }

    @Override
    public boolean contains(Object id) {
        logWarning();
        return false;
    }

    @Override
    public InternalVertex get(final Object id, final Retriever<Object, InternalVertex> retriever) {
        logWarning();
        return null;
    }

    @Override
    public void add(InternalVertex vertex, Object id) {
        logWarning();
    }

    @Override
    public List<InternalVertex> getAllNew() {
        logWarning();
        return Collections.emptyList();
    }

    @Override
    public synchronized void close() {
    }
}
