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
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.RelationTypeIndex;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.util.time.Timer;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelationIndexStatusWatcher
        extends AbstractIndexStatusWatcher<RelationIndexStatusReport, RelationIndexStatusWatcher> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationIndexStatusWatcher.class);

    private final String relationIndexName;
    private final String relationTypeName;

    public RelationIndexStatusWatcher(JanusGraph g, String relationIndexName, String relationTypeName) {
        super(g);
        this.relationIndexName = relationIndexName;
        this.relationTypeName = relationTypeName;
    }

    @Override
    protected RelationIndexStatusWatcher self() {
        return this;
    }

    /**
     * Poll a relation index until it has a certain {@link SchemaStatus},
     * or until a configurable timeout is exceeded.
     *
     * @return a report with information about schema state, execution duration, and the index
     */
    @Override
    public RelationIndexStatusReport call() throws InterruptedException {
        Preconditions.checkNotNull(g, "Graph instance must not be null");
        Preconditions.checkNotNull(relationIndexName, "Index name must not be null");
        Preconditions.checkNotNull(statuses, "Target statuses must not be null");
        Preconditions.checkArgument(statuses.size() > 0, "Target statuses must include at least one status");

        RelationTypeIndex idx;

        Timer t = new Timer(TimestampProviders.MILLI).start();
        boolean timedOut;
        while (true) {
            final SchemaStatus actualStatus;
            JanusGraphManagement management = null;
            try {
                management = g.openManagement();
                idx = management.getRelationIndex(management.getRelationType(relationTypeName), relationIndexName);
                actualStatus = idx.getIndexStatus();
                LOGGER.info("Index {} (relation type {}) has status {}", relationIndexName, relationTypeName, actualStatus);
                if (statuses.contains(actualStatus)) {
                    return new RelationIndexStatusReport(true, relationIndexName, relationTypeName, actualStatus, statuses, t.elapsed());
                }
            } finally {
                if (null != management)
                    management.rollback(); // Let an exception here propagate up the stack
            }

            timedOut = null != timeout && 0 < t.elapsed().compareTo(timeout);

            if (timedOut) {
                LOGGER.info("Timed out ({}) while waiting for index {} (relation type {}) to reach status(es) {}",
                        timeout, relationIndexName, relationTypeName, statuses);
                return new RelationIndexStatusReport(false, relationIndexName, relationTypeName, actualStatus, statuses, t.elapsed());
            }

            Thread.sleep(poll.toMillis());
        }
    }

}
