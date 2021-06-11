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

package org.janusgraph.hadoop;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.schema.Index;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.RelationTypeIndex;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.olap.job.IndexRemoveJob;
import org.janusgraph.graphdb.olap.job.IndexRepairJob;
import org.janusgraph.graphdb.olap.job.IndexUpdateJob;
import org.janusgraph.hadoop.config.JanusGraphHadoopConfiguration;
import org.janusgraph.hadoop.config.ModifiableHadoopConfiguration;
import org.janusgraph.hadoop.scan.HadoopScanMapper;
import org.janusgraph.hadoop.scan.HadoopScanRunner;
import org.janusgraph.hadoop.scan.HadoopVertexScanMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MapReduceIndexManagement {

    private static final Logger log = LoggerFactory.getLogger(MapReduceIndexManagement.class);

    private final StandardJanusGraph graph;

    private static final EnumSet<SchemaAction> SUPPORTED_ACTIONS =
            EnumSet.of(SchemaAction.REINDEX, SchemaAction.REMOVE_INDEX);

    private static final String SUPPORTED_ACTIONS_STRING =
            Joiner.on(", ").join(SUPPORTED_ACTIONS);

    public MapReduceIndexManagement(JanusGraph g) {
        this.graph = (StandardJanusGraph)g;
    }

    public JanusGraphManagement.IndexJobFuture updateIndex(Index index, SchemaAction updateAction) throws BackendException {
        return updateIndex(index, updateAction, new Configuration());
    }

    /**
     * Updates the provided index according to the given {@link SchemaAction}.
     * Only {@link SchemaAction#REINDEX} and {@link SchemaAction#REMOVE_INDEX} are supported.
     *
     * @param index the index to process
     * @param updateAction either {@code REINDEX} or {@code REMOVE_INDEX}
     * @param hadoopConf
     * @return a future that returns immediately;
     *         this method blocks until the Hadoop MapReduce job completes
     */
    // TODO make this future actually async and update javadoc @return accordingly
    public JanusGraphManagement.IndexJobFuture updateIndex(Index index, SchemaAction updateAction, Configuration hadoopConf)
            throws BackendException {

        Preconditions.checkNotNull(index, "Index parameter must not be null", index);
        Preconditions.checkNotNull(updateAction, "%s parameter must not be null", SchemaAction.class.getSimpleName());
        Preconditions.checkArgument(SUPPORTED_ACTIONS.contains(updateAction),
                "Only these %s parameters are supported: %s (was given %s)",
                SchemaAction.class.getSimpleName(), SUPPORTED_ACTIONS_STRING, updateAction);
        Preconditions.checkArgument(
                RelationTypeIndex.class.isAssignableFrom(index.getClass()) ||
                        JanusGraphIndex.class.isAssignableFrom(index.getClass()),
                "Index %s has class %s: must be a %s or %s (or subtype)",
                index.getClass(), RelationTypeIndex.class.getSimpleName(), JanusGraphIndex.class.getSimpleName());

        ModifiableHadoopConfiguration janusGraphMapReduceConfiguration =
                ModifiableHadoopConfiguration.of(JanusGraphHadoopConfiguration.MAPRED_NS, hadoopConf);

        // The job we'll execute to either REINDEX or REMOVE_INDEX
        final Class<? extends IndexUpdateJob> indexJobClass;
        final Class<? extends Mapper> mapperClass;

        // The class of the IndexUpdateJob and the Mapper that will be used to run it (VertexScanJob vs ScanJob)
        if (updateAction.equals(SchemaAction.REINDEX)) {
            indexJobClass = IndexRepairJob.class;
            mapperClass = HadoopVertexScanMapper.class;
        } else if (updateAction.equals(SchemaAction.REMOVE_INDEX)) {
            indexJobClass = IndexRemoveJob.class;
            mapperClass = HadoopScanMapper.class;
        } else {
            // Shouldn't get here -- if this exception is ever thrown, update SUPPORTED_ACTIONS
            throw new IllegalStateException("Unrecognized " + SchemaAction.class.getSimpleName() + ": " + updateAction);
        }

        // The column family that serves as input to the IndexUpdateJob
        final String readCF;
        if (RelationTypeIndex.class.isAssignableFrom(index.getClass())) {
            readCF = Backend.EDGESTORE_NAME;
        } else {
            JanusGraphIndex graphIndex = (JanusGraphIndex)index;
            if (graphIndex.isMixedIndex() && !updateAction.equals(SchemaAction.REINDEX))
                throw new UnsupportedOperationException("External mixed indexes must be removed in the indexing system directly.");

            Preconditions.checkState(JanusGraphIndex.class.isAssignableFrom(index.getClass()));
            if (updateAction.equals(SchemaAction.REMOVE_INDEX))
                readCF = Backend.INDEXSTORE_NAME;
            else
                readCF = Backend.EDGESTORE_NAME;
        }
        janusGraphMapReduceConfiguration.set(JanusGraphHadoopConfiguration.COLUMN_FAMILY_NAME, readCF);

        // The MapReduce InputFormat class based on the open graph's store manager

        HadoopStoreManager storeManager = (HadoopStoreManager) graph.getBackend().getStoreManager().getHadoopManager();
        if (storeManager == null) {
            throw new IllegalArgumentException("Store manager class " + graph.getBackend().getStoreManagerClass() + "is not supported");
        }

        final Class<? extends InputFormat> inputFormat = storeManager.getInputFormat(hadoopConf);

        // The index name and relation type name (if the latter is applicable)
        final String indexName = index.name();
        final RelationType relationType =
            RelationTypeIndex.class.isAssignableFrom(index.getClass()) ? ((RelationTypeIndex) index).getType() : null;
        final String relationTypeName = relationType == null ? StringUtils.EMPTY : relationType.name();
        Preconditions.checkNotNull(indexName);

        // Set the class of the IndexUpdateJob
        janusGraphMapReduceConfiguration.set(JanusGraphHadoopConfiguration.SCAN_JOB_CLASS, indexJobClass.getName());
        // Set the configuration of the IndexUpdateJob
        copyIndexJobKeys(hadoopConf, indexName, relationTypeName);
        janusGraphMapReduceConfiguration.set(JanusGraphHadoopConfiguration.SCAN_JOB_CONFIG_ROOT,
                GraphDatabaseConfiguration.class.getName() + "#JOB_NS");
        // Copy the StandardJanusGraph configuration under JanusGraphHadoopConfiguration.GRAPH_CONFIG_KEYS
        org.apache.commons.configuration2.Configuration localConfiguration = graph.getConfiguration().getConfigurationAtOpen();
        localConfiguration.clearProperty(Graph.GRAPH);
        copyInputKeys(hadoopConf, localConfiguration);

        String jobName = HadoopScanMapper.class.getSimpleName() + "[" + indexJobClass.getSimpleName() + "]";

        try {
            return new CompletedJobFuture(HadoopScanRunner.runJob(hadoopConf, inputFormat, jobName, mapperClass));
        } catch (Exception e) {
            return new FailedJobFuture(e);
        }
    }

    private static void copyInputKeys(org.apache.hadoop.conf.Configuration hadoopConf, org.apache.commons.configuration2.Configuration source) {
        // Copy IndexUpdateJob settings into the hadoop-backed cfg
        Iterator<String> keyIter = source.getKeys();
        while (keyIter.hasNext()) {
            String key = keyIter.next();

            String k = ConfigElement.getPath(JanusGraphHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + "." + key;
            Object vObject = source.getProperty(key);
            String v;
            if (vObject instanceof Collection) {
                v = Joiner.on(",").join((Collection<String>) vObject);
            } else {
                v = vObject.toString();
            }

            hadoopConf.set(k, v);
            log.debug("[inputkeys] Set {}={}", k, v);
        }
    }

    private static void copyIndexJobKeys(org.apache.hadoop.conf.Configuration hadoopConf, String indexName, String relationType) {
        hadoopConf.set(ConfigElement.getPath(JanusGraphHadoopConfiguration.SCAN_JOB_CONFIG_KEYS, true) + "." +
                        ConfigElement.getPath(IndexUpdateJob.INDEX_NAME), indexName);

        hadoopConf.set(ConfigElement.getPath(JanusGraphHadoopConfiguration.SCAN_JOB_CONFIG_KEYS, true) + "." +
                ConfigElement.getPath(IndexUpdateJob.INDEX_RELATION_TYPE), relationType);

        hadoopConf.set(ConfigElement.getPath(JanusGraphHadoopConfiguration.SCAN_JOB_CONFIG_KEYS, true) + "." +
                ConfigElement.getPath(GraphDatabaseConfiguration.JOB_START_TIME),
                String.valueOf(System.currentTimeMillis()));
    }

    private static class CompletedJobFuture implements JanusGraphManagement.IndexJobFuture  {

        private final ScanMetrics completedJobMetrics;

        private CompletedJobFuture(ScanMetrics completedJobMetrics) {
            this.completedJobMetrics = completedJobMetrics;
        }

        @Override
        public ScanMetrics getIntermediateResult() {
            return completedJobMetrics;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public ScanMetrics get() throws InterruptedException, ExecutionException {
            return completedJobMetrics;
        }

        @Override
        public ScanMetrics get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return completedJobMetrics;
        }
    }

    private static class FailedJobFuture implements JanusGraphManagement.IndexJobFuture {

        private final Throwable cause;

        public FailedJobFuture(Throwable cause) {
            this.cause = cause;
        }

        @Override
        public ScanMetrics getIntermediateResult() throws ExecutionException {
            throw new ExecutionException(cause);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public ScanMetrics get() throws InterruptedException, ExecutionException {
            throw new ExecutionException(cause);
        }

        @Override
        public ScanMetrics get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            throw new ExecutionException(cause);
        }
    }
}
