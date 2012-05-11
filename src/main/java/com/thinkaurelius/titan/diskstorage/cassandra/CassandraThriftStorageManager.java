package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnectionFactory;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnectionPool;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.UncheckedGenericKeyedObjectPool;
import com.thinkaurelius.titan.diskstorage.util.LocalIDManager;
import com.thinkaurelius.titan.exceptions.GraphStorageException;
import org.apache.cassandra.thrift.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY;

public class CassandraThriftStorageManager implements StorageManager {

    private static final Logger logger =
            LoggerFactory.getLogger(CassandraThriftStorageManager.class);
    
    public static final String PROP_KEYSPACE = "keyspace";
    public static final String PROP_HOSTNAME = "hostname";
    public static final String PROP_PORT = "port";
    public static final String PROP_SELF_HOSTNAME = "selfHostname";
    public static final String PROP_TIMEOUT = "thrift_timeout";

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

	private final String keyspace;
	
	private final UncheckedGenericKeyedObjectPool
			<String, CTConnection> pool;

    private final LocalIDManager idmanager;

	
	public CassandraThriftStorageManager(Configuration config) {
		this.keyspace = config.getString(PROP_KEYSPACE,DEFAULT_KEYSPACE);
		
		this.pool = CTConnectionPool.getPool(
				interpretHostname(config.getString(PROP_HOSTNAME,DEFAULT_HOSTNAME)),
				config.getInt(PROP_PORT,DEFAULT_PORT),
				config.getInt(PROP_TIMEOUT,DEFAULT_THRIFT_TIMEOUT_MS));
        idmanager = new LocalIDManager(config.getString(STORAGE_DIRECTORY_KEY) + File.separator + LocalIDManager.DEFAULT_NAME);
	}

    @Override
    public long[] getIDBlock(int partition, int blockSize) {
        return idmanager.getIDBlock(partition,blockSize);
    }


	@Override
	public TransactionHandle beginTransaction() {
		return new CassandraTransaction();
	}

	@Override
	public void close() {
        //Do nothing
	}


	@Override
	public CassandraThriftOrderedKeyColumnValueStore openDatabase(String name)
			throws GraphStorageException {
		
		CTConnection conn = null;
		try {
			conn =  pool.genericBorrowObject(keyspace);
			Cassandra.Client client = conn.getClient();
			logger.debug("Looking up metadata on keyspace {}...", keyspace);
			KsDef keyspaceDef = client.describe_keyspace(keyspace);
			boolean foundColumnFamily = false;
			for (CfDef cfDef : keyspaceDef.getCf_defs()) {
				if (cfDef.getName().equals(name)) {
					foundColumnFamily = true;
				}
			}
			if (!foundColumnFamily) {
				CfDef createColumnFamily = new CfDef();
				createColumnFamily.setName(name);
				createColumnFamily.setKeyspace(keyspace);
				createColumnFamily.setComparator_type("org.apache.cassandra.db.marshal.BytesType");
				logger.debug("Adding column family {} to keyspace {}...", name, keyspace);
                String schemaVer = null;
                try {
                    schemaVer = client.system_add_column_family(createColumnFamily);
                } catch (SchemaDisagreementException e) {
                    throw new GraphStorageException("Error in setting up column family",e);
                }
                logger.debug("Added column family {} to keyspace {}.", name, keyspace);
				
				// Try to let Cassandra converge on the new column family
				try {
					CTConnectionFactory.validateSchemaIsSettled(client, schemaVer);
				} catch (InterruptedException e) {
					throw new GraphStorageException(e);
				}
			}
		} catch (TException e) {
			throw new GraphStorageException(e);
		} catch (InvalidRequestException e) {
			throw new GraphStorageException(e);
		} catch (NotFoundException e) {
			throw new GraphStorageException(e);
		} finally {
			if (null != conn)
				pool.genericReturnObject(keyspace, conn);
		}
		
		return new CassandraThriftOrderedKeyColumnValueStore(keyspace, name, pool);
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
				logger.debug("Dropping keyspace {}...", keyspace);
				String schemaVer = client.system_drop_keyspace(keyspace);
				
				// Try to let Cassandra converge on the new column family
				CTConnectionFactory.validateSchemaIsSettled(client, schemaVer);
			} catch (NotFoundException e) {
				// Keyspace doesn't exist yet: return immediately
				logger.debug("Keyspace {} does not exist, not attempting to drop", 
						keyspace);
				return false;
			}

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
				logger.debug("Dropping keyspace {}...", keyspace);
				String schemaVer = client.system_drop_keyspace(keyspace);
				
				// Try to let Cassandra converge on the new column family
				CTConnectionFactory.validateSchemaIsSettled(client, schemaVer);
			} catch (NotFoundException e) {
				// Keyspace doesn't exist yet: return immediately
				logger.debug("Keyspace {} does not exist, not attempting to drop", 
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
}
