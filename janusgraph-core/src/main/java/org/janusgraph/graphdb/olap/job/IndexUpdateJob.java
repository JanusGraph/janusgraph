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

package org.janusgraph.graphdb.olap.job;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.schema.Index;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics.Metric;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.StandardTransactionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class IndexUpdateJob {

    protected final Logger log =
            LoggerFactory.getLogger(getClass());

    protected static final String SUCCESS_TX = "success-tx";
    protected static final String FAILED_TX = "failed-tx";

    public static final ConfigNamespace INDEX_JOB_NS = new ConfigNamespace(GraphDatabaseConfiguration.JOB_NS,"index","Configuration options relating to index jobs");

    public static final ConfigOption<String> INDEX_NAME = new ConfigOption<>(INDEX_JOB_NS,"index-name",
            "The name of the index to be repaired. For vertex-centric indexes this is the name of " +
                    "the edge label or property key on which the index is installed.",
            ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<String> INDEX_RELATION_TYPE = new ConfigOption<>(INDEX_JOB_NS,"relation-type",
            "For a vertex-centric index, this is the name of the index associated with the " +
                    "relation type configured under index-name. This should remain empty for global graph indexes.",
            ConfigOption.Type.LOCAL, "", Objects::nonNull);


    protected String indexRelationTypeName = null;
    protected String indexName = null;


    protected StandardJanusGraph graph;
    protected ManagementSystem managementSystem = null;
    protected StandardJanusGraphTx writeTx;
    protected Index index;
    protected RelationType indexRelationType;
    protected Instant jobStartTime;

    public IndexUpdateJob() { }

    protected IndexUpdateJob(IndexUpdateJob copy) {
        this.indexName = copy.indexName;
        this.indexRelationTypeName = copy.indexRelationTypeName;
    }

    public IndexUpdateJob(final String indexName, final String indexRelationTypeName) {
        this.indexName = indexName;
        this.indexRelationTypeName = indexRelationTypeName;
    }

    public boolean isGlobalGraphIndex() {
        return indexRelationTypeName ==null || StringUtils.isBlank(indexRelationTypeName);
    }

    public boolean isRelationTypeIndex() {
        return !isGlobalGraphIndex();
    }

    public void workerIterationStart(JanusGraph graph, Configuration config, ScanMetrics metrics) {
        this.graph = (StandardJanusGraph)graph;
        Preconditions.checkArgument(config.has(GraphDatabaseConfiguration.JOB_START_TIME),"Invalid configuration for this job. Start time is required.");
        this.jobStartTime = Instant.ofEpochMilli(config.get(GraphDatabaseConfiguration.JOB_START_TIME));
        if (indexName == null) {
            Preconditions.checkArgument(config.has(INDEX_NAME), "Need to configure the name of the index to be repaired");
            indexName = config.get(INDEX_NAME);
            indexRelationTypeName = config.get(INDEX_RELATION_TYPE);
            log.info("Read index information: name={} type={}", indexName, indexRelationTypeName);
        }

        try {
            this.managementSystem = (ManagementSystem)graph.openManagement();

            if (isGlobalGraphIndex()) {
                index = managementSystem.getGraphIndex(indexName);
            } else {
                indexRelationType = managementSystem.getRelationType(indexRelationTypeName);
                Preconditions.checkArgument(indexRelationType!=null,"Could not find relation type: %s", indexRelationTypeName);
                index = managementSystem.getRelationIndex(indexRelationType,indexName);
            }
            Preconditions.checkArgument(index!=null,"Could not find index: %s [%s]",indexName,indexRelationTypeName);
            log.debug("Found index {}", indexName);
            validateIndexStatus();

            StandardTransactionBuilder txb = this.graph.buildTransaction();
            txb.commitTime(jobStartTime);
            writeTx = (StandardJanusGraphTx)txb.start();
        } catch (final Exception e) {
            if (null != managementSystem && managementSystem.isOpen())
                managementSystem.rollback();
            if (writeTx!=null && writeTx.isOpen())
                writeTx.rollback();
            metrics.incrementCustom(FAILED_TX);
            throw new JanusGraphException(e.getMessage(), e);
        }
    }

    public void workerIterationEnd(ScanMetrics metrics) {
        try {
            if (null != managementSystem && managementSystem.isOpen())
                managementSystem.commit();
            if (writeTx!=null && writeTx.isOpen())
                writeTx.commit();
            metrics.incrementCustom(SUCCESS_TX);
        } catch (RuntimeException e) {
            log.error("Transaction commit threw runtime exception:", e);
            metrics.incrementCustom(FAILED_TX);
            throw e;
        } finally {
            log.info("Index {} metrics: success-tx: {} doc-updates: {} succeeded: {}", indexName,
                    metrics.getCustom(SUCCESS_TX), metrics.getCustom(IndexRepairJob.DOCUMENT_UPDATES_COUNT),
                    metrics.get(Metric.SUCCESS));
        }
    }

    protected abstract void validateIndexStatus();


}
