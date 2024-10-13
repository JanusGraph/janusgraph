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
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.Index;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.RelationTypeIndex;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.GraphIndexStatusReport;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.database.management.RelationIndexStatusReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ManagementUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ManagementUtil.class);

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

    /**
     * Force rollback all transactions which are opened on the graph.
     * @param graph - graph on which to rollback all current transactions
     */
    public static void forceRollbackAllTransactions(JanusGraph graph){
        if(graph instanceof StandardJanusGraph){
            for(JanusGraphTransaction janusGraphTransaction : ((StandardJanusGraph) graph).getOpenTransactions()){
                janusGraphTransaction.rollback();
            }
        }
    }

    /**
     * Force closes all instances except current instance. This is a dangerous operation as it will close any other active instances.
     * @param graph - graph instance to keep (this instance will not be closed).
     */
    public static void forceCloseOtherInstances(JanusGraph graph){
        String currentInstanceId = ((StandardJanusGraph) graph).getConfiguration().getUniqueGraphId();
        String currentInstanceIdWithSuffix = currentInstanceId + ManagementSystem.CURRENT_INSTANCE_SUFFIX;
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.getOpenInstances().forEach(instanceId -> {
            if(!currentInstanceId.equals(instanceId) && !currentInstanceIdWithSuffix.equals(instanceId)){
                mgmt.forceCloseInstance(instanceId);
            }
        });
        mgmt.commit();
    }

    public static void reindexAndEnableIndices(JanusGraph graph, List<String> nonEnabledGraphIndexNames, Map<String, String> nonEnabledRelationTypeIndexNamesAndTypes, long indexStatusTimeout){
        nonEnabledGraphIndexNames.forEach(indexName -> awaitGraphIndexStatus(graph, indexName, indexStatusTimeout));
        nonEnabledRelationTypeIndexNamesAndTypes.forEach((indexName, indexedElementName) -> awaitVertexCentricIndexStatus(graph, indexName, indexedElementName, indexStatusTimeout));

        nonEnabledGraphIndexNames.forEach(indexName -> {
            try {
                LOG.info("Start re-indexing graph index {}", indexName);

                ManagementSystem schemaUpdateMgmt = (ManagementSystem) graph.openManagement();
                Index index = schemaUpdateMgmt.getGraphIndex(indexName);
                schemaUpdateMgmt.updateIndex(index, SchemaAction.REINDEX).get();
                schemaUpdateMgmt.commit();

                LOG.info("Finished re-indexing graph index {}", indexName);
            } catch (Exception e) {
                LOG.error("Couldn't execute re-index for the graph index [{}]", indexName, e);
                throw new RuntimeException(e);
            }
        });

        nonEnabledRelationTypeIndexNamesAndTypes.forEach((indexName, indexedElementName) -> {
            try {
                LOG.info("Start re-indexing vertex-centric index {}", indexName);

                ManagementSystem schemaUpdateMgmt = (ManagementSystem) graph.openManagement();
                Index index = schemaUpdateMgmt.getRelationIndex(schemaUpdateMgmt.getRelationType(indexedElementName), indexName);
                schemaUpdateMgmt.updateIndex(index, SchemaAction.REINDEX).get();
                schemaUpdateMgmt.commit();

                LOG.info("Finished re-indexing vertex-centric index {}", indexName);
            } catch (Exception e) {
                LOG.error("Couldn't execute re-index for the vertex-centric index [{}]", indexName, e);
                throw new RuntimeException(e);
            }
        });
    }

    public static void forceEnableIndices(JanusGraph graph, List<String> nonEnabledGraphIndexNames, Map<String, String> nonEnabledRelationTypeIndexNamesAndTypes, long indexStatusTimeout){
        nonEnabledGraphIndexNames.forEach(indexName -> {
            try{
                awaitGraphIndexStatus(graph, indexName, indexStatusTimeout);
            } catch (Exception e){
                LOG.warn("Await for status update of the graph index {} finished with exception", indexName, e);
            }
        });
        nonEnabledRelationTypeIndexNamesAndTypes.forEach((indexName, indexedElementName) -> {
            try{
                awaitVertexCentricIndexStatus(graph, indexName, indexedElementName, indexStatusTimeout);
            } catch (Exception e){
                LOG.warn("Await for status update of the vertex-centric index {} finished with exception", indexName, e);
            }
        });

        nonEnabledGraphIndexNames.forEach(indexName -> {
            try {
                LOG.info("Start force-enabling graph index {}", indexName);

                ManagementSystem schemaUpdateMgmt = (ManagementSystem) graph.openManagement();
                Index index = schemaUpdateMgmt.getGraphIndex(indexName);
                schemaUpdateMgmt.updateIndex(index, SchemaAction.ENABLE_INDEX).get();
                schemaUpdateMgmt.commit();

                LOG.info("Finished force-enabling graph index {}", indexName);
            } catch (Exception e) {
                LOG.error("Couldn't execute force-enable for the graph index [{}]", indexName, e);
                throw new RuntimeException(e);
            }
        });

        nonEnabledRelationTypeIndexNamesAndTypes.forEach((indexName, indexedElementName) -> {
            try {
                LOG.info("Start force-enabling vertex-centric index {}", indexName);

                ManagementSystem schemaUpdateMgmt = (ManagementSystem) graph.openManagement();
                Index index = schemaUpdateMgmt.getRelationIndex(schemaUpdateMgmt.getRelationType(indexedElementName), indexName);
                schemaUpdateMgmt.updateIndex(index, SchemaAction.ENABLE_INDEX).get();
                schemaUpdateMgmt.commit();

                LOG.info("Finished force-enabling vertex-centric index {}", indexName);
            } catch (Exception e) {
                LOG.error("Couldn't execute force-enable for the vertex-centric index [{}]", indexName, e);
                throw new RuntimeException(e);
            }
        });
    }

    public static void awaitVertexCentricIndexStatus(JanusGraph janusGraph, String indexName, String indexedElement, long indexStatusTimeout) {

        RelationIndexStatusReport relationIndexStatusReport;
        try {

            relationIndexStatusReport =
                ManagementSystem.awaitRelationIndexStatus(
                    janusGraph,
                    indexName,
                    indexedElement
                ).timeout(indexStatusTimeout, ChronoUnit.MILLIS).call();

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (!relationIndexStatusReport.getSucceeded()){
            LOG.error("Await wasn't successful for index [{}]. Actual status [{}]. Time elapsed [{}]. Target statuses [{}]",
                indexName,
                relationIndexStatusReport.getActualStatus().toString(),
                relationIndexStatusReport.getElapsed().toString(),
                StringUtils.join(relationIndexStatusReport.getTargetStatuses())
            );

            throw new IllegalStateException("Couldn't await for vertex-centric index status in time [" + indexName + "]");
        }
    }

    public static void awaitGraphIndexStatus(JanusGraph janusGraph, String indexName, long indexStatusTimeout) {

        GraphIndexStatusReport graphIndexStatusReport;
        try {
            graphIndexStatusReport =
                ManagementSystem.awaitGraphIndexStatus(
                    janusGraph,
                    indexName
                ).timeout(indexStatusTimeout, ChronoUnit.MILLIS).call();

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (!graphIndexStatusReport.getSucceeded()){
            LOG.warn("Await wasn't successful for index [{}]. Covered keys [{}]. Not covered keys [{}]. Time elapsed [{}]. Target statuses [{}]",
                indexName,
                StringUtils.join(graphIndexStatusReport.getConvergedKeys()),
                StringUtils.join(graphIndexStatusReport.getNotConvergedKeys()),
                graphIndexStatusReport.getElapsed().toString(),
                StringUtils.join(graphIndexStatusReport.getTargetStatuses())
            );

            throw new IllegalStateException("Couldn't await for graph-centric index status in time [" + indexName + "]");
        }
    }

    public static boolean isIndexHasStatus(Index index, SchemaStatus status){
        if(index instanceof JanusGraphIndex){
            return isGraphIndexHasStatus((JanusGraphIndex) index, status);
        }
        if(index instanceof RelationTypeIndex){
            return isRelationIndexHasStatus((RelationTypeIndex) index, status);
        }
        throw new IllegalStateException("Unexpected index type: " + index.getClass() + ", indexName: " + index.name());
    }

    public static boolean isGraphIndexHasStatus(JanusGraphIndex graphIndex, SchemaStatus status){
        for(PropertyKey propertyKey : graphIndex.getFieldKeys()){
            if(!status.equals(graphIndex.getIndexStatus(propertyKey))){
                return false;
            }
        }
        return true;
    }

    public static boolean isRelationIndexHasStatus(RelationTypeIndex relationTypeIndex, SchemaStatus status){
        return status.equals(relationTypeIndex.getIndexStatus());
    }

}
