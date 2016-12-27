package com.thinkaurelius.titan.diskstorage.util;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StoreMetaData;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.util.stats.MetricManager;
import static com.thinkaurelius.titan.diskstorage.util.MetricInstrumentedStore.*;

import java.util.List;
import java.util.Map;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.METRICS_MERGE_STORES;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class MetricInstrumentedStoreManager implements KeyColumnValueStoreManager {

    public static final String M_OPEN_DATABASE = "openDatabase";
    public static final String M_START_TX = "startTransaction";
    public static final String M_CLOSE_MANAGER = "closeManager";


    public static final String GLOBAL_PREFIX = "global";

    private final KeyColumnValueStoreManager backend;
    private final boolean mergeStoreMetrics;
    private final String mergedMetricsName;
    private final String managerMetricsName;

    public MetricInstrumentedStoreManager(KeyColumnValueStoreManager backend, String managerMetricsName,
                                          boolean mergeStoreMetrics, String mergedMetricsName) {
        this.backend = backend;
        this.mergeStoreMetrics = mergeStoreMetrics;
        this.mergedMetricsName = mergedMetricsName;
        this.managerMetricsName = managerMetricsName;
    }


    private String getMetricsStoreName(String storeName) {
        return mergeStoreMetrics ? mergedMetricsName : storeName;
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        MetricManager.INSTANCE.getCounter(GLOBAL_PREFIX, managerMetricsName, M_OPEN_DATABASE, M_CALLS).inc();
        return new MetricInstrumentedStore(backend.openDatabase(name, metaData),getMetricsStoreName(name));
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        if (!txh.getConfiguration().hasGroupName()) {
            backend.mutateMany(mutations,txh);
        }
        String prefix = txh.getConfiguration().getGroupName();

        final MetricManager mgr = MetricManager.INSTANCE;
        mgr.getCounter(prefix, managerMetricsName, M_MUTATE, M_CALLS).inc();
        final Timer.Context tc = mgr.getTimer(prefix,  managerMetricsName, M_MUTATE, M_TIME).time();

        try {
            backend.mutateMany(mutations,txh);
        } catch (BackendException e) {
            mgr.getCounter(prefix,  managerMetricsName, M_MUTATE, M_EXCEPTIONS).inc();
            throw e;
        } catch (RuntimeException e) {
            mgr.getCounter(prefix,  managerMetricsName, M_MUTATE, M_EXCEPTIONS).inc();
            throw e;
        } finally {
            tc.stop();
        }
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        MetricManager.INSTANCE.getCounter(GLOBAL_PREFIX, managerMetricsName, M_START_TX, M_CALLS).inc();
        return backend.beginTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        backend.close();
        MetricManager.INSTANCE.getCounter(GLOBAL_PREFIX, managerMetricsName, M_CLOSE_MANAGER, M_CALLS).inc();
    }

    @Override
    public void clearStorage() throws BackendException {
        backend.clearStorage();
    }

    @Override
    public StoreFeatures getFeatures() {
        return backend.getFeatures();
    }

    @Override
    public String getName() {
        return backend.getName();
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        return backend.getLocalKeyPartition();
    }
}
