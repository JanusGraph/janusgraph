package com.thinkaurelius.titan.diskstorage.hazelcast;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.ByteArraySerializer;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.common.LocalStoreManager;
import com.thinkaurelius.titan.diskstorage.common.NoOpStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.FileStorageConfiguration;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHazelcastStoreManager extends LocalStoreManager implements StoreManager {

    private static final Logger logger = LoggerFactory.getLogger(AbstractHazelcastStoreManager.class);

    protected final HazelcastInstance manager;
    protected final FileStorageConfiguration storageConfig;
    protected final StoreFeatures features = getDefaultFeatures();
    protected final long lockExpireMS;

    public AbstractHazelcastStoreManager(Configuration config) throws StorageException {
        super(config);
        Config conf = new ClasspathXmlConfig("hazelcast.xml");
        SerializerConfig sc = new SerializerConfig();
        sc.setImplementation(new StaticBufferSerializer()).setTypeClass(StaticBuffer.class);
        conf.getSerializationConfig().addSerializerConfig(sc);
        manager = Hazelcast.newHazelcastInstance(conf);
        storageConfig = new FileStorageConfiguration(directory);
        lockExpireMS = config.getLong(GraphDatabaseConfiguration.LOCK_EXPIRE_MS,
                GraphDatabaseConfiguration.LOCK_EXPIRE_MS_DEFAULT);

        if (transactional)
            logger.warn("Hazelcast does not support multiple transactions per thread");
    }

    @Override
    public StoreTransaction beginTransaction(StoreTxConfig txConfig) throws StorageException {
        if (transactional) {
            TransactionOptions txo = new TransactionOptions();
            txo.setTimeout(lockExpireMS, TimeUnit.MILLISECONDS);
            return new HazelCastTransaction(manager.newTransactionContext(txo), txConfig);
        } else {
            return new NoOpStoreTransaction(txConfig);
        }
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public String getConfigurationProperty(String key) throws StorageException {
        return storageConfig.getConfigurationProperty(key);
    }

    @Override
    public void setConfigurationProperty(String key, String value) throws StorageException {
        storageConfig.setConfigurationProperty(key, value);
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + ":" + directory.toString();
    }

    private StoreFeatures getDefaultFeatures() {
        StoreFeatures features = new StoreFeatures();

        features.supportsOrderedScan = false;
        features.supportsUnorderedScan = true;
        features.supportsBatchMutation = false;
        features.supportsMultiQuery = false;

        features.supportsTransactions = true;
        features.supportsConsistentKeyOperations = false;
        features.supportsLocking = false;

        features.isDistributed = false;
        features.isKeyOrdered = false;
        features.hasLocalKeyPartition = false;

        return features;
    }

    private static class HazelCastTransaction extends AbstractStoreTransaction {
        private final TransactionContext context;

        public HazelCastTransaction(TransactionContext context, StoreTxConfig txConfig) {
            super(txConfig);

            this.context = context;
            this.context.beginTransaction();
        }

        @Override
        public void commit() {
            context.commitTransaction();
        }

        @Override
        public void rollback() {
            context.rollbackTransaction();
        }
    }


    private static class StaticBufferSerializer implements ByteArraySerializer<StaticBuffer> {

        @Override
        public byte[] write(StaticBuffer o) throws IOException {
            return o.as(StaticBuffer.ARRAY_FACTORY);
        }

        @Override
        public StaticBuffer read(byte[] bytes) throws IOException {
            return new StaticArrayBuffer(bytes);
        }

        @Override
        public int getTypeId() {
            return 1;
        }

        @Override
        public void destroy() {
            //Do nothing
        }
    }
}
