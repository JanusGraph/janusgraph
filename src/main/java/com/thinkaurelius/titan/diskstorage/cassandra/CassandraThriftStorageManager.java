package com.thinkaurelius.titan.diskstorage.cassandra;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.idmanagement.OrderedKeyColumnValueIDManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransactionHandle;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.LocalLockMediator;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnectionFactory;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnectionPool;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.UncheckedGenericKeyedObjectPool;
import com.thinkaurelius.titan.diskstorage.util.ConfigHelper;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * This class creates {@see CassandraThriftOrderedKeyColumnValueStore}s and
 * handles Cassandra-backed allocation of vertex IDs for Titan (when so
 * configured).
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 * 
 */
public class CassandraThriftStorageManager extends AbstractCassandraStoreManager {

    private static final Logger log =
            LoggerFactory.getLogger(CassandraThriftStorageManager.class);
    

    
    public static ConcurrentHashMap<String, CassandraThriftOrderedKeyColumnValueStore> stores =
    		new ConcurrentHashMap<String, CassandraThriftOrderedKeyColumnValueStore>();


    /**
     * Default column family used for ID block management.
     * <p>
     * Value = {@value}
     */
    public static final String idCfName = "id_allocations";


	private final UncheckedGenericKeyedObjectPool
			<String, CTConnection> pool;

    private final OrderedKeyColumnValueIDManager idmanager;
    
    private final int lockRetryCount;
    
    private final long lockWaitMS, lockExpireMS;
    
    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;
    
    private final String llmPrefix;


	public CassandraThriftStorageManager(Configuration config) throws StorageException {
		super(config);

		
		this.pool = CTConnectionPool.getPool(
				hostname,
				port,
				config.getInt(GraphDatabaseConfiguration.COMMUNICATION_TIMEOUT_KEY, GraphDatabaseConfiguration.COMMUNICATION_TIMEOUT_DEFAULT));
		




        idmanager = new OrderedKeyColumnValueIDManager(
        		openDatabase("titan_ids", keyspace, null, null), rid, config);

        features = CassandraFeatures.of(config);
	}

    @Override
    public StorageFeatures getFeatures() {
        return features;
    }

    @Override
    public void setIDBlockSizer(IDBlockSizer sizer) {
        idmanager.setIDBlockSizer(sizer);
    }

    @Override
    public long[] getIDBlock(int partition) throws StorageException {
        return idmanager.getIDBlock(partition);
    }

	@Override
	public StoreTransactionHandle beginTransaction() {
		return new CassandraTransaction();
	}

	@Override
	public void close() {
		stores.clear();
	}

    @Override
	public CassandraThriftOrderedKeyColumnValueStore openDatabase(final String name)
			throws StorageException {
		
		CassandraThriftOrderedKeyColumnValueStore lockStore =
				openDatabase(name + "_locks", keyspace, null, null);

		CassandraThriftOrderedKeyColumnValueStore dataStore =
				openDatabase(name, keyspace, lockStore, llm);
		
		return dataStore;
	
	}
	
	private CassandraThriftOrderedKeyColumnValueStore openDatabase(final String name, final String ksoverride, CassandraThriftOrderedKeyColumnValueStore lockStore, LocalLockMediator llm)
			throws StorageException {
		
		String storeKey = llmPrefix + ":" + ksoverride + ":" + name;
	
		CassandraThriftOrderedKeyColumnValueStore store =
				stores.get(storeKey);
		
		if (null != store) {
			return store;
		}

		CTConnection conn = null;
		try {
			KsDef keyspaceDef = ensureKeyspaceExists(ksoverride);
			
			conn =  pool.genericBorrowObject(ksoverride);
			Cassandra.Client client = conn.getClient();
			log.debug("Looking up metadata on keyspace {}...", ksoverride);
			boolean foundColumnFamily = false;
			for (CfDef cfDef : keyspaceDef.getCf_defs()) {
				String curCfName = cfDef.getName();
				if (curCfName.equals(name)) {
					foundColumnFamily = true;
				}
			}
			if (!foundColumnFamily) {
				log.debug("Keyspace {} not found, about to create it", ksoverride);
				createColumnFamily(client, ksoverride, name);
			} else {
				log.debug("Found keyspace: {}", ksoverride);
			}
		} catch (TException e) {
			throw new PermanentStorageException(e);
		} catch (InvalidRequestException e) {
			throw new PermanentStorageException(e);
		} catch (NotFoundException e) {
			throw new PermanentStorageException(e);
		} catch (SchemaDisagreementException e) {
			throw new TemporaryStorageException(e);
		} finally {
			if (null != conn)
				pool.genericReturnObject(ksoverride, conn);
		}
		
		store = new CassandraThriftOrderedKeyColumnValueStore(ksoverride, name,
				pool, readConsistencyLevel, writeConsistencyLevel, lockStore,
				llm, rid, lockRetryCount, lockWaitMS, lockExpireMS);
		stores.putIfAbsent(storeKey, store);
		store = stores.get(storeKey);
		log.debug("Created {}", store);
		
		return store;
	}
	
	
	/**
	 * Connect to Cassandra via Thrift on the specified host and
	 * port and attempt to drop the named keyspace.
	 * 
	 * This is a utility method intended mainly for testing.  It is
	 * equivalent to issuing "drop keyspace {@code <keyspace>};" in
	 * the cassandra-cli tool.
	 * 
	 * @throws RuntimeException if any checked Thrift or UnknownHostException
	 *         is thrown in the body of this method
	 */
	public void clearStorage() throws StorageException {
		CTConnection conn = null;
		try {
			conn = CTConnectionPool.getFactory(hostname, port, GraphDatabaseConfiguration.COMMUNICATION_TIMEOUT_DEFAULT).makeRawConnection();

			Cassandra.Client client = conn.getClient();
			
			try {
                stores.clear();

				CTConnectionPool.getPool(hostname, port, GraphDatabaseConfiguration.COMMUNICATION_TIMEOUT_DEFAULT).clear(keyspace);
                
				client.describe_keyspace(keyspace);
				// Keyspace must exist
				log.debug("Dropping keyspace {}...", keyspace);
				String schemaVer = client.system_drop_keyspace(keyspace);
				
				// Try to let Cassandra converge on the new column family
				CTConnectionFactory.validateSchemaIsSettled(client, schemaVer);
				
			} catch (NotFoundException e) {
				// Keyspace doesn't exist yet: return immediately
				log.debug("Keyspace {} does not exist, not attempting to drop", 
						keyspace);
			}
		} catch (Exception e) {
			throw new TemporaryStorageException(e);
		} finally {
			if (null != conn && conn.getTransport().isOpen())
				conn.getTransport().close();
		}
	}
	
	private KsDef ensureKeyspaceExists(String name)
			throws NotFoundException, InvalidRequestException, TException,
			SchemaDisagreementException, StorageException {
		
		CTConnectionFactory fac = 
				CTConnectionPool.getFactory(hostname, port, GraphDatabaseConfiguration.COMMUNICATION_TIMEOUT_DEFAULT);
		
		CTConnection conn = fac.makeRawConnection();
		Cassandra.Client client = conn.getClient();
		
		try {
			
			try {
				client.set_keyspace(name);
				log.debug("Found existing keyspace {}", name);
			} catch (InvalidRequestException e) {
				// Keyspace didn't exist; create it
				log.debug("Creating keyspace {}...", name);
				KsDef ksdef = new KsDef();
				ksdef.setName(name);
				ksdef.setStrategy_class("org.apache.cassandra.locator.SimpleStrategy");
				Map<String, String> stratops = new HashMap<String, String>();
				stratops.put("replication_factor", String.valueOf(replicationFactor));
				ksdef.setStrategy_options(stratops);
				ksdef.setCf_defs(new LinkedList<CfDef>()); // cannot be null but can be empty

				String schemaVer = client.system_add_keyspace(ksdef);
				// Try to block until Cassandra converges on the new keyspace
				try {
					CTConnectionFactory.validateSchemaIsSettled(client, schemaVer);
				} catch (InterruptedException ie) {
					throw new TemporaryStorageException(ie);
				}
			}

			return client.describe_keyspace(name);
			
		} finally {
			if (null != conn && null != conn.getTransport() && conn.getTransport().isOpen()) {
				conn.getTransport().close();
			}
		}
	}
    
    private void createColumnFamily(Cassandra.Client client, String ksname, String cfName)
    		throws InvalidRequestException, TException, StorageException {
		CfDef createColumnFamily = new CfDef();
		createColumnFamily.setName(cfName);
		createColumnFamily.setKeyspace(ksname);
		createColumnFamily.setComparator_type("org.apache.cassandra.db.marshal.BytesType");
		
		// Hard-coded caching settings
		if (cfName.equals(GraphDatabaseConfiguration.STORAGE_EDGESTORE_NAME)) {
			createColumnFamily.setCaching("keys_only");
		} else if (cfName.equals(GraphDatabaseConfiguration.STORAGE_PROPERTYINDEX_NAME)) {
			createColumnFamily.setCaching("rows_only");
		}
		
		log.debug("Adding column family {} to keyspace {}...", cfName, ksname);
        String schemaVer = null;
        try {
            schemaVer = client.system_add_column_family(createColumnFamily);
        } catch (SchemaDisagreementException e) {
            throw new TemporaryStorageException("Error in setting up column family",e);
        }
        log.debug("Added column family {} to keyspace {}.", cfName, ksname);
		
		// Try to let Cassandra converge on the new column family
		try {
			CTConnectionFactory.validateSchemaIsSettled(client, schemaVer);
		} catch (InterruptedException e) {
			throw new TemporaryStorageException(e);
		}
    	
    }

	@Override
	public String toString() {
		return "CassandraThriftStorageManager[ks=" + keyspace + "]";
	}

    static com.netflix.astyanax.model.ConsistencyLevel getConsistency(CassandraTransaction.Consistency consistency) {
        switch (consistency) {
            case ONE: return com.netflix.astyanax.model.ConsistencyLevel.CL_ONE;
            case TWO: return com.netflix.astyanax.model.ConsistencyLevel.CL_TWO;
            case THREE: return com.netflix.astyanax.model.ConsistencyLevel.CL_THREE;
            case ANY: return com.netflix.astyanax.model.ConsistencyLevel.CL_ANY;
            case ALL: return com.netflix.astyanax.model.ConsistencyLevel.CL_ALL;
            case QUORUM: return com.netflix.astyanax.model.ConsistencyLevel.CL_QUORUM;
            case LOCAL_QUORUM: return com.netflix.astyanax.model.ConsistencyLevel.CL_LOCAL_QUORUM;
            case EACH_QUORUM: return com.netflix.astyanax.model.ConsistencyLevel.CL_EACH_QUORUM;
            default: throw new IllegalArgumentException("Unknown consistency level: " + consistency);
        }
    }
}
