package com.thinkaurelius.titan.diskstorage.cassandra;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnectionFactory;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnectionPool;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.UncheckedGenericKeyedObjectPool;
import com.thinkaurelius.titan.exceptions.GraphStorageException;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class CassandraThriftStorageManager implements StorageManager {

    private static final Logger log =
            LoggerFactory.getLogger(CassandraThriftStorageManager.class);
    
    public static final String PROP_KEYSPACE = "keyspace";
    public static final String PROP_HOSTNAME = "hostname";
    public static final String PROP_PORT = "port";
    public static final String PROP_SELF_HOSTNAME = "selfHostname";
    public static final String PROP_TIMEOUT = "thrift_timeout";
    public static final String PROP_ID_KEYSPACE = "id_keyspace";
    
    public static Map<String, CassandraThriftOrderedKeyColumnValueStore> stores =
    		new ConcurrentHashMap<String, CassandraThriftOrderedKeyColumnValueStore>();
    
    /**
     * Default name for the Cassandra keyspace
     * <p>
     * Value = {@value}
     */
    public static final String DEFAULT_KEYSPACE = "titantest00";

    /**
     * Default hostname at which to attempt Cassandra Thrift connection.
     * <p>
     * Value = {@value}
     */
    public static final String DEFAULT_HOSTNAME = null;

    /**
     * Default canonical hostname of the local machine.
     * <p>
     * Value = {@value}
     */
    public static final String DEFAULT_SELF_HOSTNAME = null;

    /**
     * Default timeout for Thrift TSocket objects used to
     * connect to the Cassandra cluster.
     * <p>
     * Value = {@value}
     */
    public static final int DEFAULT_THRIFT_TIMEOUT_MS = 10000;

    /**
     * Default port at which to attempt Cassandra Thrift connection.
     * <p>
     * Value = {@value}
     */
    public static final int DEFAULT_PORT = 9160;
    
    /**
     * Default column family used for ID block management.
     * <p>
     * Value = {@value}
     */
    public static final String idCfName = "id_allocations";
    
    /**
     * Default keyspace to be used for ID block management.
     * <p>
     * Value = {@value}
     */
    public static final String ID_KEYSPACE = "titan_ids";


	private final String keyspace;
	
	private final UncheckedGenericKeyedObjectPool
			<String, CTConnection> pool;

    private final CassandraIDManager idmanager;
    
    private final int lockRetryCount;
    
    private final long lockWaitMS, lockExpireMS;
    
    private static final byte[] rid;
	
	public CassandraThriftStorageManager(Configuration config) {
		this.keyspace = config.getString(PROP_KEYSPACE,DEFAULT_KEYSPACE);
		
		this.pool = CTConnectionPool.getPool(
				interpretHostname(config.getString(PROP_HOSTNAME,DEFAULT_HOSTNAME)),
				config.getInt(PROP_PORT,DEFAULT_PORT),
				config.getInt(PROP_TIMEOUT,DEFAULT_THRIFT_TIMEOUT_MS));
		
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
		
        idmanager = new CassandraIDManager(openDatabase("blocks_allocated", ID_KEYSPACE, false), rid);
	}
	
	static {
		
		byte[] addrBytes;
		try {
			addrBytes = InetAddress.getLocalHost().getAddress();
		} catch (UnknownHostException e) {
			throw new GraphStorageException(e);
		}
		byte[] procNameBytes = ManagementFactory.getRuntimeMXBean().getName().getBytes();
		
		rid = new byte[addrBytes.length + procNameBytes.length];
		System.arraycopy(addrBytes, 0, rid, 0, addrBytes.length);
		System.arraycopy(procNameBytes, 0, rid, addrBytes.length, procNameBytes.length);
	}

    @Override
    public long[] getIDBlock(int partition, int blockSize) {
        return idmanager.getIDBlock(partition,blockSize);
    }


	@Override
	public TransactionHandle beginTransaction() {
		return new CassandraTransaction(this, rid, 
				lockRetryCount,
				lockWaitMS,
				lockExpireMS);
	}

	@Override
	public void close() {
        //Do nothing
	}

	@Override
	public CassandraThriftOrderedKeyColumnValueStore openDatabase(final String name)
			throws GraphStorageException {
		return openDatabase(name, keyspace, true);
	
	}
	
	private CassandraThriftOrderedKeyColumnValueStore openDatabase(final String name, final String ksoverride, boolean createLockColumnFamily)
			throws GraphStorageException {
	
		CassandraThriftOrderedKeyColumnValueStore store =
				stores.get(name);
		
		if (null != store) {
			return store;
		}

		CTConnection conn = null;
		try {
			conn =  pool.genericBorrowObject(ksoverride);
			Cassandra.Client client = conn.getClient();
			log.debug("Looking up metadata on keyspace {}...", ksoverride);
			KsDef keyspaceDef = client.describe_keyspace(ksoverride);
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
		} finally {
			if (null != conn)
				pool.genericReturnObject(ksoverride, conn);
		}
		
		store = new CassandraThriftOrderedKeyColumnValueStore(ksoverride, name, pool, this);
		stores.put(name, store);
		log.debug("Created {}", store);
		
		return store;
	}
	
	String getLockColumnFamilyName(String cfName) {
		return cfName + "_locks";
	}
	
	/**
	 * Drop the named keyspace if it exists.  Otherwise, do nothing.
	 * 
	 * @throws GraphStorageException wrapping any unexpected Exception or
	 *         subclass of Exception
	 * @returns true if the keyspace was dropped, false if it was not present
	 */
	public boolean dropKeyspace(String keyspace) throws GraphStorageException {
		CTConnection conn = null;
		try {
			conn =  pool.genericBorrowObject(keyspace);
			Cassandra.Client client = conn.getClient();
			
			try {
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
				return false;
			}

                        stores.clear();
			return true;
		} catch (Exception e) {
			throw new GraphStorageException(e);
		} finally {
			if (null != conn)
				pool.genericReturnObject(keyspace, conn);
		}

	}
	
	
	/**
	 * Connect to Cassandra via Thrift on the specified host and
	 * port and attempt to drop the named keyspace.
	 * 
	 * This is a utility method intended mainly for testing.  It is
	 * equivalent to issuing "drop keyspace {@code <keyspace>};" in
	 * the cassandra-cli tool.
	 * 
	 * @param keyspace the keyspace to drop
	 * @throws RuntimeException if any checked Thrift or UnknownHostException
	 *         is thrown in the body of this method
	 */
	public static void dropKeyspace(String keyspace, String hostname, int port)
		throws GraphStorageException {
		CTConnection conn = null;
		try {
			conn = CTConnectionPool.getFactory(hostname, port, DEFAULT_THRIFT_TIMEOUT_MS).makeRawConnection();

			Cassandra.Client client = conn.getClient();
			
			try {
				client.describe_keyspace(keyspace);
				// Keyspace must exist
				log.debug("Dropping keyspace {}...", keyspace);
				String schemaVer = client.system_drop_keyspace(keyspace);
				
				// Try to let Cassandra converge on the new column family
				CTConnectionFactory.validateSchemaIsSettled(client, schemaVer);

                                stores.clear();
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
	
	/**
	 * CassandraThriftOrderedKeyColumnValueStore instances call this method
	 * when their own close() method is invoked.  This method removes a
	 * close()ed store from the {@code stores} map on this object, so that
	 * it can't be returned in future calls to openDatabase().
	 * 
	 * This method is idempotent, so a store may call it multiple times; only
	 * the first invocation will do anything.
	 * 
	 * @param cf Column family name of a closing store
	 * @param storeToClose the closing store
	 */
//	void closeStore(String cf, CassandraThriftOrderedKeyColumnValueStore storeToClose) {
//		assert null != cf;
//		assert null != storeToClose;
//		
//		CassandraThriftOrderedKeyColumnValueStore s = stores.get(cf);
//		
//		if (null == s) {
//			log.debug("Store already closed: {}", storeToClose);
//		} else if (s.equals(storeToClose)) {
//			stores.remove(cf);
//			log.debug("Closed store: {}", storeToClose);
//		} else {
//			log.warn("Attempted to close {} which is unknown to its StorageManager={}; Database references leaking?",
//					storeToClose, this);
//		}
//	}

    /**
     * If hostname is non-null, returns hostname.
     *
     * If hostname is null, returns the result of calling
     * InetAddress.getLocalHost().getCanonicalHostName().
     * Any exceptions generated during said call are rethrown as
     * RuntimeException.
     *
     * @throws RuntimeException in case of UnknownHostException for localhost
     * @return sanitized hostname
     */
    private static String interpretHostname(String hostname) {
        if (null == hostname) {
            try {
                return InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        } else {
            return hostname;
        }
    }
    
    private void createColumnFamily(Cassandra.Client client, String ksname, String cfName)
    		throws InvalidRequestException, TException {
		CfDef createColumnFamily = new CfDef();
		createColumnFamily.setName(cfName);
		createColumnFamily.setKeyspace(ksname);
		createColumnFamily.setComparator_type("org.apache.cassandra.db.marshal.BytesType");
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
