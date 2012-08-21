package com.thinkaurelius.titan.diskstorage.cassandra;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnectionFactory;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnectionPool;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.UncheckedGenericKeyedObjectPool;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediators;
import com.thinkaurelius.titan.diskstorage.util.ConfigHelper;
import com.thinkaurelius.titan.diskstorage.util.OrderedKeyColumnValueIDManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * This class creates {@see CassandraThriftOrderedKeyColumnValueStore}s and
 * handles Cassandra-backed allocation of vertex IDs for Titan (when so
 * configured).
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 * 
 */
public class CassandraThriftStorageManager implements StorageManager {

    private static final Logger log =
            LoggerFactory.getLogger(CassandraThriftStorageManager.class);
    

    
    public static ConcurrentHashMap<String, CassandraThriftOrderedKeyColumnValueStore> stores =
    		new ConcurrentHashMap<String, CassandraThriftOrderedKeyColumnValueStore>();
    
    /**
     * Default name for the Cassandra keyspace
     * <p>
     * Value = {@value}
     */
    public static final String KEYSPACE_DEFAULT = "titan";
    public static final String KEYSPACE_KEY = "keyspace";


    /**
     * Default hostname at which to attempt Cassandra Thrift connection.
     * <p>
     * Value = {@value}
     */
    public static final String HOSTNAME_DEFAULT = "127.0.0.1";


    /**
     * Default timeout for Thrift TSocket objects used to
     * connect to the Cassandra cluster.
     * <p>
     * Value = {@value}
     */
    public static final int THRIFT_TIMEOUT_DEFAULT = 10000;
    public static final String THRIFT_TIMEOUT_KEY = "thrift-timeout";

    /**
     * Default port at which to attempt Cassandra Thrift connection.
     * <p>
     * Value = {@value}
     */
    public static final int PORT_DEFAULT = 9160;



    /**
     * Default column family used for ID block management.
     * <p>
     * Value = {@value}
     */
    public static final String idCfName = "id_allocations";
    
    public static final String READ_CONSISTENCY_LEVEL_KEY = "read-consistency-level";
    public static final String READ_CONSISTENCY_LEVEL_DEFAULT = "QUORUM";
    
    public static final String WRITE_CONSISTENCY_LEVEL_KEY = "write-consistency-level";
    /*
     * Any operation attempted with ConsistencyLevel.TWO
     * against a single-node Cassandra cluster (like the one
     * we use in a lot of our test cases) will fail with
     * an UnavailableException.  In other words, if you
     * set TWO here, Cassandra will require TWO nodes, even
     * if only one node has ever been a member of the
     * cluster in question.
     */
    public static final String WRITE_CONSISTENCY_LEVEL_DEFAULT = "QUORUM";
    
    public static final String REPLICATION_FACTOR_KEY = "replication-factor";
    public static final int REPLICATION_FACTOR_DEFAULT  = 1;

	private final String keyspace;
	
	private final UncheckedGenericKeyedObjectPool
			<String, CTConnection> pool;

    private final OrderedKeyColumnValueIDManager idmanager;
    
    private final int lockRetryCount;
    
    private final long lockWaitMS, lockExpireMS;
    
    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;
    
    private final String llmPrefix;
    
    private final byte[] rid;
    
    private final String hostname;
    private final int port;
    
    private final int replicationFactor;
	
	public CassandraThriftStorageManager(Configuration config) {
		
		this.rid = ConfigHelper.getRid(config);
		
		this.keyspace = config.getString(KEYSPACE_KEY, KEYSPACE_DEFAULT);
		
		this.hostname = config.getString(HOSTNAME_KEY, HOSTNAME_DEFAULT);
		this.port = config.getInt(PORT_KEY, PORT_DEFAULT);
		
		this.pool = CTConnectionPool.getPool(
				hostname,
				port,
				config.getInt(THRIFT_TIMEOUT_KEY, THRIFT_TIMEOUT_DEFAULT));
		
		this.llmPrefix =
				config.getString(
						LOCAL_LOCK_MEDIATOR_PREFIX_KEY,
						getClass().getName());
		
		this.replicationFactor = 
				config.getInt(
						REPLICATION_FACTOR_KEY,
						REPLICATION_FACTOR_DEFAULT);
		
		this.lockRetryCount =
				config.getInt(
						GraphDatabaseConfiguration.LOCK_RETRY_COUNT,
						GraphDatabaseConfiguration.LOCK_RETRY_COUNT_DEFAULT);
		
		this.lockWaitMS =
				config.getLong(
						GraphDatabaseConfiguration.LOCK_WAIT_MS,
						GraphDatabaseConfiguration.LOCK_WAIT_MS_DEFAULT);
		
		this.lockExpireMS =
				config.getLong(
						GraphDatabaseConfiguration.LOCK_EXPIRE_MS,
						GraphDatabaseConfiguration.LOCK_EXPIRE_MS_DEFAULT);
		
		this.readConsistencyLevel = ConsistencyLevel.valueOf(
				config.getString(
						READ_CONSISTENCY_LEVEL_KEY,
						READ_CONSISTENCY_LEVEL_DEFAULT));
		
		log.debug("Set read consistency level to {}", this.readConsistencyLevel);
		
		this.writeConsistencyLevel = ConsistencyLevel.valueOf(
				config.getString(
						WRITE_CONSISTENCY_LEVEL_KEY,
						WRITE_CONSISTENCY_LEVEL_DEFAULT));
		
		log.debug("Set write consistency level to {}", this.writeConsistencyLevel);
		
        idmanager = new OrderedKeyColumnValueIDManager(
        		openDatabase("titan_ids", keyspace, null, null), rid, config);
	}

    @Override
    public long[] getIDBlock(int partition) {
        return idmanager.getIDBlock(partition);
    }

	@Override
	public TransactionHandle beginTransaction() {
		return new CassandraTransaction();
	}

	@Override
	public void close() {
		stores.clear();
	}

    @Override
	public CassandraThriftOrderedKeyColumnValueStore openDatabase(final String name)
			throws GraphStorageException {
		
		CassandraThriftOrderedKeyColumnValueStore lockStore =
				openDatabase(name + "_locks", keyspace, null, null);
		LocalLockMediator llm = LocalLockMediators.INSTANCE.get(llmPrefix + ":" + name);
		CassandraThriftOrderedKeyColumnValueStore dataStore =
				openDatabase(name, keyspace, lockStore, llm);
		
		return dataStore;
	
	}
	
	private CassandraThriftOrderedKeyColumnValueStore openDatabase(final String name, final String ksoverride, CassandraThriftOrderedKeyColumnValueStore lockStore, LocalLockMediator llm)
			throws GraphStorageException {
		
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
			throw new GraphStorageException(e);
		} catch (InvalidRequestException e) {
			throw new GraphStorageException(e);
		} catch (NotFoundException e) {
			throw new GraphStorageException(e);
		} catch (SchemaDisagreementException e) {
			throw new GraphStorageException(e);
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
	public void clearStorage() throws GraphStorageException {
		CTConnection conn = null;
		try {
			conn = CTConnectionPool.getFactory(hostname, port, THRIFT_TIMEOUT_DEFAULT).makeRawConnection();

			Cassandra.Client client = conn.getClient();
			
			try {
                stores.clear();

				CTConnectionPool.getPool(hostname, port, THRIFT_TIMEOUT_DEFAULT).clear(keyspace);
                
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
			throw new GraphStorageException(e);
		} finally {
			if (null != conn && conn.getTransport().isOpen())
				conn.getTransport().close();
		}
	}
	
	private KsDef ensureKeyspaceExists(String name)
			throws NotFoundException, InvalidRequestException, TException,
			SchemaDisagreementException {
		
		CTConnectionFactory fac = 
				CTConnectionPool.getFactory(hostname, port, THRIFT_TIMEOUT_DEFAULT);
		
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
					throw new GraphStorageException(ie);
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
    		throws InvalidRequestException, TException {
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
            throw new GraphStorageException("Error in setting up column family",e);
        }
        log.debug("Added column family {} to keyspace {}.", cfName, ksname);
		
		// Try to let Cassandra converge on the new column family
		try {
			CTConnectionFactory.validateSchemaIsSettled(client, schemaVer);
		} catch (InterruptedException e) {
			throw new GraphStorageException(e);
		}
    	
    }

	@Override
	public String toString() {
		return "CassandraThriftStorageManager[ks=" + keyspace + "]";
	}
}
