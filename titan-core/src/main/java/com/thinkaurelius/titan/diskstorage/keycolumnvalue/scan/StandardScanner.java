package com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.StandardBaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardScanner  {

    private final KeyColumnValueStoreManager manager;

    public StandardScanner(final KeyColumnValueStoreManager manager) {
        Preconditions.checkArgument(manager!=null);
        Preconditions.checkArgument(manager.getFeatures().hasScan(),"Provided data store does not support scans: %s",manager);

        this.manager = manager;
    }

    public Builder build() {
        return new Builder();
    }


    public class Builder {

        private ScanJob job;
        private int numProcessingThreads;
        private TimestampProvider times;
        private Configuration configuration;
        private String dbName;

        private Builder() {
            numProcessingThreads = 1;
            job = null;
            times = null;
            configuration = Configuration.EMPTY;
            dbName = null;
        }

        public Builder setNumProcessingThreads(int numThreads) {
            Preconditions.checkArgument(numThreads>0,
                    "Need to specify a positive number of processing threads: %s",numThreads);
            this.numProcessingThreads = numThreads;
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

        public Builder setJob(ScanJob job) {
            Preconditions.checkArgument(job!=null);
            this.job = job;
            return this;
        }

        public Builder setConfiguration(Configuration config) {
            Preconditions.checkArgument(config!=null);
            this.configuration = config;
            return this;
        }

        public Future<ScanMetrics> execute() throws BackendException {
            Preconditions.checkArgument(job!=null,"Need to specify a job to execute");
            Preconditions.checkArgument(StringUtils.isNotBlank(dbName),"Need to specify a database to execute against");
            Preconditions.checkArgument(times!=null,"Need to configure the timestamp provider for this job");
            StandardBaseTransactionConfig.Builder txBuilder = new StandardBaseTransactionConfig.Builder();
            txBuilder.timestampProvider(times);
            if (configuration!=Configuration.EMPTY) {
                txBuilder.customOptions(configuration);

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
            try {
                StandardScannerExecutor executor = new StandardScannerExecutor(job, kcvs, storeTx,
                        manager.getFeatures(), numProcessingThreads, configuration);
                new Thread(executor).start();
                return executor;
            } catch (Throwable e) {
                storeTx.rollback();
                kcvs.close();
                throw e;
            }
        }

    }

}
