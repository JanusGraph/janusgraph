package com.thinkaurelius.titan.core.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.RelationTypeIndex;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;


import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ManagementUtil {

    /**
     * This method blocks and waits until the provided index has been updated across the entire Titan cluster
     * and reached a stable state.
     * This method will wait for the given period of time and throw an exception if the index did not reach a
     * final state within that time. The method simply returns when the index has reached the final state
     * prior to the time period expiring.
     *
     * This is a utility method to be invoked between two {@link com.thinkaurelius.titan.core.schema.TitanManagement#updateIndex(TitanIndex, com.thinkaurelius.titan.core.schema.SchemaAction)} calls
     * to ensure that the previous update has successfully persisted.
     *
     * @param g
     * @param indexName
     * @param time
     * @param unit
     */
    public static void awaitGraphIndexUpdate(TitanGraph g, String indexName, long time, TemporalUnit unit) {
        awaitIndexUpdate(g,indexName,null,time,unit);
    }

    public static void awaitVertexIndexUpdate(TitanGraph g, String indexName, String relationTypeName, long time, TemporalUnit unit) {
        awaitIndexUpdate(g,indexName,relationTypeName,time,unit);
    }

    private static void awaitIndexUpdate(TitanGraph g, String indexName, String relationTypeName, long time, TemporalUnit unit) {
        Preconditions.checkArgument(g!=null && g.isOpen(),"Need to provide valid, open graph instance");
        Preconditions.checkArgument(time>0 && unit!=null,"Need to provide valid time interval");
        Preconditions.checkArgument(StringUtils.isNotBlank(indexName),"Need to provide an index name");
        StandardTitanGraph graph = (StandardTitanGraph)g;
        TimestampProvider times = graph.getConfiguration().getTimestampProvider();
        Instant end = times.getTime().plus(Duration.of(time,unit));
        boolean isStable = false;
        while (times.getTime().isBefore(end)) {
            TitanManagement mgmt = graph.openManagement();
            try {
                if (StringUtils.isNotBlank(relationTypeName)) {
                    RelationTypeIndex idx = mgmt.getRelationIndex(mgmt.getRelationType(relationTypeName)
                            ,indexName);
                    Preconditions.checkArgument(idx!=null,"Index could not be found: %s @ %s",indexName,relationTypeName);
                    isStable = idx.getIndexStatus().isStable();
                } else {
                    TitanGraphIndex idx = mgmt.getGraphIndex(indexName);
                    Preconditions.checkArgument(idx!=null,"Index could not be found: %s",indexName);
                    isStable = true;
                    for (PropertyKey key : idx.getFieldKeys()) {
                        if (!idx.getIndexStatus(key).isStable()) isStable = false;
                    }
                }
            } finally {
                mgmt.rollback();
            }
            if (isStable) break;
            try {
                times.sleepFor(Duration.ofMillis(500));
            } catch (InterruptedException e) {

            }
        }
        if (!isStable) throw new TitanException("Index did not stabilize within the given amount of time. For sufficiently long " +
                "wait periods this is most likely caused by a failed/incorrectly shut down Titan instance or a lingering transaction.");
    }

}
