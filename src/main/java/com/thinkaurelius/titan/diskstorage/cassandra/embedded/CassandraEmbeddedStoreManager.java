package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;
import org.apache.cassandra.config.*;
import org.apache.cassandra.config.CFMetaData.Caching;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.scheduler.IRequestScheduler;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;

public class CassandraEmbeddedStoreManager extends AbstractCassandraStoreManager {

    private static final Logger log = LoggerFactory.getLogger(CassandraEmbeddedStoreManager.class);

	/**
	 * When non-empty, the CassandraEmbeddedStoreManager constructor will copy
	 * the value to the "cassandra.config" system property and start a
	 * backgrounded cassandra daemon thread. cassandra's static initializers
	 * will interpret the "cassandra.config" system property as a url pointing
	 * to a cassandra.yaml file.
	 * <p>
	 * An example value of this variable is
	 * "file:///home/dalaro/titan/target/cassandra-tmp/conf/127.0.0.1/cassandra.yaml".
	 * <p>
	 * When empty, the constructor does none of the steps described above.
	 * <p>
	 * The constructor logic described above is also internally synchronized in
	 * order to start Cassandra at most once in a thread-safe manner. Subsequent
	 * constructor invocations (or concurrent invocations which enter the
	 * internal synchronization block after the first) with a nonempty value for
	 * this variable will behave as though an empty value was set.
	 * <p>
	 * Value = {@value}
	 */
    public static final String CASSANDRA_CONFIG_DIR_DEFAULT = "";
    public static final String CASSANDRA_CONFIG_DIR_KEY = "cassandra-config-dir";

    private final String cassandraConfigDir;

    private final Map<String,CassandraEmbeddedKeyColumnValueStore> openStores;

    private final IRequestScheduler requestScheduler;

    public CassandraEmbeddedStoreManager(Configuration config) throws StorageException {
		super(config);
		this.cassandraConfigDir =
				config.getString(
						CASSANDRA_CONFIG_DIR_KEY,
						CASSANDRA_CONFIG_DIR_DEFAULT);

        
        if (null != cassandraConfigDir && !cassandraConfigDir.isEmpty()) {
        	CassandraDaemonWrapper.start(cassandraConfigDir);
        }



        openStores = new HashMap<String,CassandraEmbeddedKeyColumnValueStore>(8);

        this.requestScheduler = DatabaseDescriptor.getRequestScheduler();

        //Determine if key ordered
        try {
            Token<?> token = StorageService.instance.getLocalPrimaryRange().left;
            if (token instanceof BytesToken) {
                features.hasLocalKeyPartition=true;
                features.isKeyOrdered=true;
            }
        } catch (Exception e) { log.warn("Could not read local token range"); }
	}



    @Override
    public String toString() {
        return "embeddedCassandra"+super.toString();
    }

	@Override
	public void close() {
		openStores.clear();
	}

    @Override
    public synchronized KeyColumnValueStore openDatabase(String name)
            throws StorageException {
        if (openStores.containsKey(name)) return openStores.get(name);
        else {
            // Ensure that both the keyspace and column family exist
            ensureKeyspaceExists(keySpaceName);
            ensureColumnFamilyExists(keySpaceName, name);

            CassandraEmbeddedKeyColumnValueStore store = new CassandraEmbeddedKeyColumnValueStore(keySpaceName,
                    name, this);
            openStores.put(name,store);
            return store;
        }
    }

    ByteBuffer[] getLocalKeyPartition() throws StorageException {
        // getLocalPrimaryRange() returns a raw type
        @SuppressWarnings("rawtypes")
        Range<Token> range = StorageService.instance.getLocalPrimaryRange();
        Token<?> leftKeyExclusive = range.left;
        Token<?> rightKeyInclusive = range.right;

        if (leftKeyExclusive instanceof BytesToken) {
            assert rightKeyInclusive instanceof BytesToken;

            // l is exclusive, r is inclusive
            BytesToken l = (BytesToken)leftKeyExclusive;
            BytesToken r = (BytesToken)rightKeyInclusive;
            
            Preconditions.checkArgument(l.token.length==r.token.length,"Tokens have unequal length");
            int tokenLength = l.token.length;
            
            // Convert l and r into unsigned BigIntegers
            BigInteger le = new BigInteger(1, l.token);
            BigInteger ri = new BigInteger(1, r.token);

            BigInteger modulo = BigInteger.ONE.shiftLeft(8 * tokenLength);

            BigInteger leftInclusive = le.add(BigInteger.ONE).mod(modulo);
            BigInteger rightExclusive = ri.add(BigInteger.ONE).mod(modulo);

            ByteBuffer lb = ByteBuffer.wrap(leftInclusive.toByteArray());
            ByteBuffer rb = ByteBuffer.wrap(rightExclusive.toByteArray());
            Preconditions.checkArgument(lb.remaining()==tokenLength);
            Preconditions.checkArgument(rb.remaining()==tokenLength);

            return new ByteBuffer[]{ lb, rb };
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /*
      * This implementation can't handle counter columns.
      *
      * The private method internal_batch_mutate in CassandraServer as of 1.1.3
      * provided most of the following method after transaction handling.
      */
    @Override
    public void mutateMany(Map<String, Map<ByteBuffer, Mutation>> mutations, StoreTransaction txh) throws StorageException {
        Preconditions.checkNotNull(mutations);

        long deletionTimestamp = TimeUtility.getApproxNSSinceEpoch(false);
        long additionTimestamp = TimeUtility.getApproxNSSinceEpoch(true);

        int size = 0;
        for (Map<ByteBuffer,Mutation> mutation: mutations.values()) size+=mutation.size();
        Map<ByteBuffer,RowMutation> rowMutations = new HashMap<ByteBuffer,RowMutation>(size);

        for (Map.Entry<String,Map<ByteBuffer,Mutation>> mutEntry : mutations.entrySet()) {
            String columnFamily = mutEntry.getKey();
            for (Map.Entry<ByteBuffer, Mutation> titanMutation : mutEntry.getValue().entrySet()) {
                ByteBuffer key = titanMutation.getKey().duplicate();
                Mutation mut = titanMutation.getValue();

                RowMutation rm = rowMutations.get(key);
                if (rm==null) {
                    rm = new RowMutation(keySpaceName, key);
                    rowMutations.put(key,rm);
                }
 
                if (mut.hasAdditions()) {
                    for (Entry e : mut.getAdditions()) {
                        QueryPath path = new QueryPath(columnFamily, null, e.getColumn().duplicate());
                        rm.add(path, e.getValue().duplicate(), additionTimestamp);
                    }
                }

                if (mut.hasDeletions()) {
                    for (ByteBuffer col : mut.getDeletions()) {
                        QueryPath path = new QueryPath(columnFamily, null, col.duplicate());
                        rm.delete(path, deletionTimestamp);
                    }
                }

            }
        }

        ConsistencyLevel clvl = getTx(txh).getWriteConsistencyLevel().getThriftConsistency();
        List<RowMutation> mutationList = new ArrayList<RowMutation>(rowMutations.values());
        rowMutations=null;
        mutate(mutationList, clvl);
    }

    private void mutate(List<RowMutation> cmds, ConsistencyLevel clvl) throws StorageException {
        try {
            schedule(DatabaseDescriptor.getRpcTimeout());
            try {
                StorageProxy.mutate(cmds, clvl);
            } finally {
                release();
            }
        } catch (UnavailableException ex) {
            throw new TemporaryStorageException(ex);
        } catch (TimeoutException ex) {
            log.debug("Cassandra TimeoutException", ex);
            throw new TemporaryStorageException(ex);
        }
    }

    private void schedule(long timeoutMS) throws TimeoutException {
        requestScheduler.queue(Thread.currentThread(), "default", DatabaseDescriptor.getRpcTimeout());
    }

    /**
     * Release count for the used up resources
     */
    private void release() {
        requestScheduler.release();
    }

	@Override
	public void clearStorage() throws StorageException {
		openStores.clear();
		try {
            if (Schema.instance.getTableInstance(keySpaceName)!=null)
			    MigrationManager.announceKeyspaceDrop(keySpaceName);
		} catch (ConfigurationException e) {
			throw new PermanentStorageException(e);
		}
	}


	private void ensureKeyspaceExists(String keyspaceName) throws StorageException {
		
		if (null != Schema.instance.getTableInstance(keyspaceName))
			return;
		
		// Keyspace not found; create it
		String strategyName = "org.apache.cassandra.locator.SimpleStrategy";
		Map<String, String> options = new HashMap<String, String>();
		options.put("replication_factor", String.valueOf(replicationFactor));
		KSMetaData ksm;
		try {
			ksm = KSMetaData.newKeyspace(keyspaceName, strategyName, options);
		} catch (ConfigurationException e) {
			throw new PermanentStorageException("Failed to instantiate keyspace metadata for " + keyspaceName, e);
		}
		try {
			MigrationManager.announceNewKeyspace(ksm);
		} catch (ConfigurationException e) {
			throw new PermanentStorageException("Failed to create keyspace " + keyspaceName, e);
		}
	}
	
	private void ensureColumnFamilyExists(String keyspaceName, String columnfamilyName) throws StorageException {
		if (null != Schema.instance.getCFMetaData(keyspaceName, columnfamilyName))
			return;
		
		// Column Family not found; create it
		CFMetaData cfm = new CFMetaData(keyspaceName, columnfamilyName, ColumnFamilyType.Standard, BytesType.instance, null);
		
		// Hard-coded caching settings
		if (columnfamilyName.startsWith(Backend.EDGESTORE_NAME)) {
			cfm.caching(Caching.KEYS_ONLY);
		} else if (columnfamilyName.startsWith(Backend.VERTEXINDEX_STORE_NAME)) {
			cfm.caching(Caching.ROWS_ONLY);
		}

		try {
			cfm.addDefaultIndexNames();
		} catch (ConfigurationException e) {
			throw new PermanentStorageException("Failed to create column family metadata for " + keyspaceName + ":" + columnfamilyName, e);
		}
		try {
			MigrationManager.announceNewColumnFamily(cfm);
		} catch (ConfigurationException e) {
			throw new PermanentStorageException("Failed to create column family " + keyspaceName + ":" + columnfamilyName, e);
		}
	}

}
