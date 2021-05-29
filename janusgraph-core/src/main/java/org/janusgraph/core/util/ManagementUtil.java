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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.Index;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.RelationTypeIndex;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ManagementUtil {

    /**
     * This method blocks and waits until the provided index has been updated across the entire JanusGraph cluster
     * and reached a stable state.
     * This method will wait for the given period of time and throw an exception if the index did not reach a
     * final state within that time. The method simply returns when the index has reached the final state
     * prior to the time period expiring.
     *
     * This is a utility method to be invoked between two {@link org.janusgraph.core.schema.JanusGraphManagement#updateIndex(Index, org.janusgraph.core.schema.SchemaAction)} calls
     * to ensure that the previous update has successfully persisted.
     *
     * @param g
     * @param indexName
     * @param time
     * @param unit
     */
    public static void awaitGraphIndexUpdate(JanusGraph g, String indexName, long time, TemporalUnit unit) {
        awaitIndexUpdate(g,indexName,null,time,unit);
    }

    public static void awaitVertexIndexUpdate(JanusGraph g, String indexName, String relationTypeName, long time, TemporalUnit unit) {
        awaitIndexUpdate(g,indexName,relationTypeName,time,unit);
    }

    private static void awaitIndexUpdate(JanusGraph g, String indexName, String relationTypeName, long time, TemporalUnit unit) {
        Preconditions.checkArgument(g!=null && g.isOpen(),"Need to provide valid, open graph instance");
        Preconditions.checkArgument(time>0 && unit!=null,"Need to provide valid time interval");
        Preconditions.checkArgument(StringUtils.isNotBlank(indexName),"Need to provide an index name");
        StandardJanusGraph graph = (StandardJanusGraph)g;
        TimestampProvider times = graph.getConfiguration().getTimestampProvider();
        Instant end = times.getTime().plus(Duration.of(time,unit));
        boolean isStable = false;
        while (times.getTime().isBefore(end)) {
            JanusGraphManagement management = graph.openManagement();
            try {
                if (StringUtils.isNotBlank(relationTypeName)) {
                    RelationTypeIndex idx = management.getRelationIndex(management.getRelationType(relationTypeName)
                            ,indexName);
                    Preconditions.checkNotNull(idx, "Index could not be found: %s @ %s",indexName,relationTypeName);
                    isStable = idx.getIndexStatus().isStable();
                } else {
                    JanusGraphIndex idx = management.getGraphIndex(indexName);
                    Preconditions.checkNotNull(idx, "Index could not be found: %s",indexName);
                    isStable = true;
                    for (PropertyKey key : idx.getFieldKeys()) {
                        if (!idx.getIndexStatus(key).isStable()) isStable = false;
                    }
                }
            } finally {
                management.rollback();
            }
            if (isStable) break;
            try {
                times.sleepFor(Duration.ofMillis(500));
            } catch (InterruptedException ignored) {

            }
        }
        if (!isStable) throw new JanusGraphException("Index did not stabilize within the given amount of time. For sufficiently long " +
                "wait periods this is most likely caused by a failed/incorrectly shut down JanusGraph instance or a lingering transaction.");
    }

}
