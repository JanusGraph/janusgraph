package com.thinkaurelius.titan.graphdb.database.management;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.RelationTypeIndex;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.util.time.Timer;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProviders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelationIndexStatusWatcher
        extends AbstractIndexStatusWatcher<RelationIndexStatusReport, RelationIndexStatusWatcher> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationIndexStatusWatcher.class);

    private String relationIndexName;
    private String relationTypeName;

    public RelationIndexStatusWatcher(TitanGraph g, String relationIndexName, String relationTypeName) {
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
        Preconditions.checkNotNull(status, "Target status must not be null");

        RelationTypeIndex idx;

        Timer t = new Timer(TimestampProviders.MILLI).start();
        boolean timedOut;
        while (true) {
            SchemaStatus actualStatus = null;
            TitanManagement mgmt = null;
            try {
                mgmt = g.openManagement();
                idx = mgmt.getRelationIndex(mgmt.getRelationType(relationTypeName), relationIndexName);
                actualStatus = idx.getIndexStatus();
                LOGGER.info("Index {} (relation type {}) has status {}", relationIndexName, relationTypeName, actualStatus);
                if (status.equals(actualStatus)) {
                    return new RelationIndexStatusReport(true, relationIndexName, relationTypeName, actualStatus, status, t.elapsed());
                }
            } finally {
                if (null != mgmt)
                    mgmt.rollback(); // Let an exception here propagate up the stack
            }

            timedOut = null != timeout && 0 < t.elapsed().compareTo(timeout);

            if (timedOut) {
                LOGGER.info("Timed out ({}) while waiting for index {} (relation type {}) to reach status {}",
                        timeout, relationIndexName, relationTypeName, status);
                return new RelationIndexStatusReport(false, relationIndexName, relationTypeName, actualStatus, status, t.elapsed());
            }

            Thread.sleep(poll.toMillis());
        }
    }

}
