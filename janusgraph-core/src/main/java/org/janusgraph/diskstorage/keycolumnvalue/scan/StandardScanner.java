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

package org.janusgraph.diskstorage.keycolumnvalue.scan;

import com.google.common.base.Preconditions;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.*;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardScanner  {

    private final KeyColumnValueStoreManager manager;
    private final Set<KeyColumnValueStore> openStores;
    private final ConcurrentMap<Object,StandardScannerExecutor> runningJobs;
    private final AtomicLong jobCounter;

    public StandardScanner(final KeyColumnValueStoreManager manager) {
        Preconditions.checkArgument(manager!=null);
        Preconditions.checkArgument(manager.getFeatures().hasScan(),"Provided data store does not support scans: %s",manager);

        this.manager = manager;
        this.openStores = new HashSet<>(4);
        this.runningJobs = new ConcurrentHashMap<>();
        this.jobCounter = new AtomicLong(0);
    }

    public Builder build() {
        return new Builder();
    }

    public void close() throws BackendException {
        //Interrupt running jobs
        for (StandardScannerExecutor exe : runningJobs.values()) {
            if (exe.isCancelled() || exe.isDone()) continue;
            exe.cancel(true);
        }
        for (KeyColumnValueStore kcvs : openStores) kcvs.close();
    }

    private void addJob(Object jobId, StandardScannerExecutor executor) {
        for (Map.Entry<Object,StandardScannerExecutor> jobs : runningJobs.entrySet()) {
            StandardScannerExecutor exe = jobs.getValue();
            if (exe.isDone() || exe.isCancelled()) {
                runningJobs.remove(jobs.getKey(),exe);
            }
        }
        Preconditions.checkArgument(runningJobs.putIfAbsent(jobId, executor) == null,"Another job with the same id is already running: %s",jobId);
    }

    public JanusGraphManagement.IndexJobFuture getRunningJob(Object jobId) {
        return runningJobs.get(jobId);
    }

    public class Builder {

        private static final int DEFAULT_WORKBLOCK_SIZE = 10000;

        private ScanJob job;
        private int numProcessingThreads;
        private int workBlockSize;
        private TimestampProvider times;
        private Configuration graphConfiguration;
        private Configuration jobConfiguration;
        private String dbName;
        private Consumer<ScanMetrics> finishJob;
        private Object jobId;

        private Builder() {
            numProcessingThreads = 1;
            workBlockSize = DEFAULT_WORKBLOCK_SIZE;
            job = null;
            times = null;
            graphConfiguration = Configuration.EMPTY;
            jobConfiguration = Configuration.EMPTY;
            dbName = null;
            jobId = Long.valueOf(jobCounter.incrementAndGet());
            finishJob = m -> {} ;
        }

        public Builder setNumProcessingThreads(int numThreads) {
            Preconditions.checkArgument(numThreads>0,
                    "Need to specify a positive number of processing threads: %s",numThreads);
            this.numProcessingThreads = numThreads;
            return this;
        }

        public Builder setWorkBlockSize(int size) {
            Preconditions.checkArgument(size>0, "Need to specify a positive work block size: %s",size);
            this.workBlockSize = size;
            return this;
        }

        public Builder setTimestampProvider(TimestampProvider times) {
            Preconditions.checkArgument(times!=null);
            this.times=times;
            return this;
        }

        public Builder setStoreName(String name) {
            Preconditions.checkArgument(StringUtils.isNotBlank(name),"Invalid name: %s",name);
            this.dbName = name;
            return this;
        }

        public Object getJobId() {
            return jobId;
        }

        public Builder setJobId(Object id) {
            Preconditions.checkArgument(id!=null,"Need to provide a valid id: %s",id);
            this.jobId = id;
            return this;
        }

        public Builder setJob(ScanJob job) {
            Preconditions.checkArgument(job!=null);
            this.job = job;
            return this;
        }

        public Builder setGraphConfiguration(Configuration config) {
            Preconditions.checkArgument(config!=null);
            this.graphConfiguration = config;
            return this;
        }

        public Builder setJobConfiguration(Configuration config) {
            Preconditions.checkArgument(config!=null);
            this.jobConfiguration = config;
            return this;
        }

        public Configuration getJobConfiguration() {
            return this.jobConfiguration;
        }

        public Builder setFinishJob(Consumer<ScanMetrics> finishJob) {
            Preconditions.checkArgument(finishJob != null);
            this.finishJob = finishJob;
            return this;
        }

        public JanusGraphManagement.IndexJobFuture execute() throws BackendException {
            Preconditions.checkArgument(job!=null,"Need to specify a job to execute");
            Preconditions.checkArgument(StringUtils.isNotBlank(dbName),"Need to specify a database to execute against");
            Preconditions.checkArgument(times!=null,"Need to configure the timestamp provider for this job");
            StandardBaseTransactionConfig.Builder txBuilder = new StandardBaseTransactionConfig.Builder();
            txBuilder.timestampProvider(times);

            Configuration scanConfig = manager.getFeatures().getScanTxConfig();
            if (Configuration.EMPTY != graphConfiguration) {
                scanConfig = null == scanConfig ?
                        graphConfiguration :
                        new MergedConfiguration(graphConfiguration, scanConfig);
            }
            if (null != scanConfig) {
                txBuilder.customOptions(scanConfig);
            }

//            if (!txOptions.isEmpty()) {
//                ModifiableConfiguration writeConf = GraphDatabaseConfiguration.buildConfiguration();
//                for (Map.Entry<String,Object> confEntry : txOptions.entrySet()) {
//                    writeConf.set(
//                            (ConfigOption<Object>) ConfigElement.parse(ROOT_NS, confEntry.getKey()).element,
//                            confEntry.getValue());
//                }
//                Configuration customConf = writeConf;
//                if (configuration!=Configuration.EMPTY) {
//                    customConf = new MergedConfiguration(writeConf, configuration);
//
//                }
//                txBuilder.customOptions(customConf);
//            }

            StoreTransaction storeTx = manager.beginTransaction(txBuilder.build());
            KeyColumnValueStore kcvs = manager.openDatabase(dbName);

            openStores.add(kcvs);
            try {
                StandardScannerExecutor executor = new StandardScannerExecutor(job, finishJob, kcvs, storeTx,
                        manager.getFeatures(), numProcessingThreads, workBlockSize, jobConfiguration, graphConfiguration);
                addJob(jobId,executor);
                new Thread(executor).start();
                return executor;
            } catch (Throwable e) {
                storeTx.rollback();
                throw e;
            }
        }

    }
}
