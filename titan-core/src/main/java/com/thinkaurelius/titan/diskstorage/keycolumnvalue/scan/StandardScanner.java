package com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.StandardBaseTransactionConfig;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardScanner  {

    private final KeyColumnValueStoreManager manager;
    private final Configuration config;

    public StandardScanner(final KeyColumnValueStoreManager manager,
                           final Configuration configuration) {
        Preconditions.checkArgument(manager!=null && configuration!=null);
        Preconditions.checkArgument(manager.getFeatures().hasScan(),"Provided data store does not support scans: %s",manager);

        this.manager = manager;
        this.config = configuration;
    }

    public Builder build() {
        return new Builder();
    }


    public class Builder {

        private ScanJob job;
        private Map<String,Object> txOptions;
        private int numProcessingThreads;
        private Configuration configuration;
        private String dbName;

        private Builder() {
            numProcessingThreads = 1;
            job = null;
            txOptions = new HashMap<>(4);
            configuration = Configuration.EMPTY;
            dbName = null;
        }

        public Builder setNumProcessingThreads(int numThreads) {
            Preconditions.checkArgument(numThreads>0,
                    "Need to specify a positive number of processing threads: %s",numThreads);
            this.numProcessingThreads = numThreads;
            return this;
        }

        public Builder setCustomTxOptions(String key, Object value) {
            Preconditions.checkArgument(StringUtils.isNotBlank(key) && value!=null);
            txOptions.put(key,value);
            return this;
        }

        public Builder setDatabaseName(String name) {
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
            StandardBaseTransactionConfig.Builder txBuilder = new StandardBaseTransactionConfig.Builder();
            txBuilder.timestampProvider(config.get(TIMESTAMP_PROVIDER));
            if (!txOptions.isEmpty()) {
                ModifiableConfiguration writeConf = GraphDatabaseConfiguration.buildConfiguration();
                for (Map.Entry<String,Object> confEntry : txOptions.entrySet()) {
                    writeConf.set(
                            (ConfigOption<Object>) ConfigElement.parse(ROOT_NS, confEntry.getKey()).element,
                            confEntry.getValue());
                }
                //TODO: Merge in graph database configuration based custom options?
                Configuration customConf = writeConf;
                if (configuration!=Configuration.EMPTY) {
                    customConf = new MergedConfiguration(writeConf, configuration);

                }
                txBuilder.customOptions(customConf);
            }
            StoreTransaction storeTx = manager.beginTransaction(txBuilder.build());
            KeyColumnValueStore kcvs = manager.openDatabase(dbName);
            StandardScannerExecutor executor = new StandardScannerExecutor(job,kcvs,storeTx,
                            manager.getFeatures(),numProcessingThreads, configuration);
            new Thread(executor).start();
            return executor;
        }

    }

}
