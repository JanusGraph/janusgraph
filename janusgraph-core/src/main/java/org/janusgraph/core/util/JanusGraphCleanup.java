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

package org.janusgraph.core.util;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.BackendException;

/**
 * Utility class containing methods that simplify JanusGraph clean-up processes.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphCleanup {

    /**
     * Drop graph database, deleting all data in storage and indexing backends. Graph can be open or closed (will be
     * closed as part of the drop operation). The graph is also removed from the {@link org.janusgraph.graphdb.management.JanusGraphManager}
     * graph reference tracker, if there.
     *
     * <p><b>WARNING: This is an irreversible operation that will delete all graph and index data.</b></p>
     *
     * @param graph
     * @throws BackendException If an error occurs during deletion
     * @deprecated Use {@link org.janusgraph.core.JanusGraphFactory#drop(JanusGraph)}
     */
    @Deprecated
    public static void clear(JanusGraph graph) throws BackendException {
        JanusGraphFactory.drop(graph);
    }
}
