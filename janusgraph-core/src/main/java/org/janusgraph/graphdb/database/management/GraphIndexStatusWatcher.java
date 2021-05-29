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

package org.janusgraph.graphdb.database.management;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.util.time.Timer;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class GraphIndexStatusWatcher
        extends AbstractIndexStatusWatcher<GraphIndexStatusReport, GraphIndexStatusWatcher> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphIndexStatusWatcher.class);

    private final String graphIndexName;

    public GraphIndexStatusWatcher(JanusGraph g, String graphIndexName) {
        super(g);
        this.graphIndexName = graphIndexName;
    }

    @Override
    protected GraphIndexStatusWatcher self() {
        return this;
    }

    @Override
    public GraphIndexStatusReport call() throws InterruptedException {
        Preconditions.checkNotNull(g, "Graph instance must not be null");
        Preconditions.checkNotNull(graphIndexName, "Index name must not be null");
        Preconditions.checkNotNull(statuses, "Target statuses must not be null");
        Preconditions.checkArgument(statuses.size() > 0, "Target statuses must include at least one status");

        Map<String, SchemaStatus> notConverged = new HashMap<>();
        Map<String, SchemaStatus> converged = new HashMap<>();
        JanusGraphIndex idx;

        Timer t = new Timer(TimestampProviders.MILLI).start();
        boolean timedOut;
        while (true) {
            JanusGraphManagement management = null;
            try {
                management = g.openManagement();
                idx = management.getGraphIndex(graphIndexName);
                for (PropertyKey pk : idx.getFieldKeys()) {
                    SchemaStatus s = idx.getIndexStatus(pk);
                    LOGGER.debug("Key {} has status {}", pk, s);
                    if (!statuses.contains(s))
                        notConverged.put(pk.toString(), s);
                    else
                        converged.put(pk.toString(), s);
                }
            } finally {
                if (null != management)
                    management.rollback(); // Let an exception here propagate up the stack
            }

            if (!notConverged.isEmpty()) {
                String waitingOn = StringUtils.join(notConverged, "=", ",");
                LOGGER.info("Some key(s) on index {} do not currently have status(es) {}: {}", graphIndexName, statuses, waitingOn);
            } else {
                LOGGER.info("All {} key(s) on index {} have status(es) {}", converged.size(), graphIndexName, statuses);
                return new GraphIndexStatusReport(true, graphIndexName, statuses, notConverged, converged, t.elapsed());
            }

            timedOut = null != timeout && 0 < t.elapsed().compareTo(timeout);

            if (timedOut) {
                LOGGER.info("Timed out ({}) while waiting for index {} to converge on status(es) {}",
                        timeout, graphIndexName, statuses);
                return new GraphIndexStatusReport(false, graphIndexName, statuses, notConverged, converged, t.elapsed());
            }
            notConverged.clear();
            converged.clear();

            Thread.sleep(poll.toMillis());
        }
    }
}

