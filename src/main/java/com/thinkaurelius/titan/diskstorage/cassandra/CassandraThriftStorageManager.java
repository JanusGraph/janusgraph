package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.configuration.CassandraStorageConfiguration;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnectionFactory;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnectionPool;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.UncheckedGenericKeyedObjectPool;
import com.thinkaurelius.titan.exceptions.GraphStorageException;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraThriftStorageManager implements StorageManager {
	
	private final CassandraThriftNodeIDMapper mapper;
	
	private final String keyspace; // convenience copy of sc.getKeyspace()
	
	private final UncheckedGenericKeyedObjectPool
			<String, CTConnection> pool;
	
	private static final Logger logger =
		LoggerFactory.getLogger(CassandraThriftStorageManager.class);
	
	public CassandraThriftStorageManager(CassandraStorageConfiguration sc) {
		this.keyspace = sc.getKeyspace();
		
		this.pool = CTConnectionPool.getPool(
				sc.getRuntimeHostname(),
				sc.getPort(),
				sc.getThriftTimeoutMS());
		
		this.mapper = new CassandraThriftNodeIDMapper(
				keyspace,
				sc.getRuntimeHostname(),
				sc.getPort(),
				sc.getRuntimeSelfHostname(),
				sc.getThriftTimeoutMS());
	}

	@Override
	public TransactionHandle beginTransaction() {
		return new CassandraTransaction();
	}

	@Override
	public void close() {
		// Do nothing
	}

	@Override
	public CassandraThriftOrderedKeyColumnValueStore openDatabase(String name) throws GraphStorageException {
		return openOrderedDatabase(name);
	}

	@Override
	public CassandraThriftOrderedKeyColumnValueStore openOrderedDatabase(String name)
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
		
		return new CassandraThriftOrderedKeyColumnValueStore(keyspace, name, pool, mapper);
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
			conn = CTConnectionPool.getFactory(hostname, port, CassandraStorageConfiguration.DEFAULT_THRIFT_TIMEOUT_MS).makeRawConnection();

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
}
