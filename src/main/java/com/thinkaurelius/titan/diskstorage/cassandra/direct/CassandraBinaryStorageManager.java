package com.thinkaurelius.titan.diskstorage.cassandra.direct;

import com.thinkaurelius.titan.diskstorage.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction;
import com.thinkaurelius.titan.exceptions.GraphStorageException;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;

public class CassandraBinaryStorageManager implements StorageManager {
	
	private static final Logger logger = 
		LoggerFactory.getLogger(CassandraBinaryStorageManager.class);
		
	private static CassandraDaemon cassandra;
	private static boolean cassandraStopped;
	private static CassandraServer cassandraServer;
	
	private final String keyspace;

	public CassandraBinaryStorageManager(String keyspace) {
		this.keyspace = keyspace;

		synchronized (CassandraBinaryStorageManager.class) {
			if (null == cassandra) {
				try {
					// We need the cassandra reference in close() later
					cassandra = startDaemon();
				} catch (IOException e) {
					throw new GraphStorageException(e);
				}

				// Create keyspace if DNE
				cassandraServer = new CassandraServer();
				try {
					KsDef existing = cassandraServer.describe_keyspace(this.keyspace);
					if (null == existing)
						throw new NotFoundException();
				} catch (NotFoundException e) {
					KsDef addKs = new KsDef();
					addKs.setName(this.keyspace);
					addKs.setStrategy_class("org.apache.cassandra.locator.SimpleStrategy");
					addKs.setReplication_factor(1);
					addKs.setCf_defs(new LinkedList<CfDef>()); // cannot be null but can be empty
					try {
						cassandraServer.system_add_keyspace(addKs);
					} catch (InvalidRequestException e1) {
						throw new GraphStorageException(e);
					} catch (TException e1) {
						throw new GraphStorageException(e);
					}
				} catch (InvalidRequestException e) {
					throw new GraphStorageException(e);
				}
				try {
					cassandraServer.set_keyspace(keyspace);
				} catch (InvalidRequestException e) {
					throw new GraphStorageException(e);
				} catch (TException e) {
					throw new GraphStorageException(e);
				}
			}
			
			if (cassandraStopped) {
				cassandra.start();
			}
		}
	}
	
	@Override
	public KeyColumnValueStore openDatabase(String name) throws GraphStorageException {
		return openOrderedDatabase(name);
	}

	@Override
	public OrderedKeyColumnValueStore openOrderedDatabase(String name)
			throws GraphStorageException {
		
		return new CassandraBinaryOrderedKeyColumnValueStore(keyspace, name);
	}

	@Override
	public TransactionHandle beginTransaction() {
		return new CassandraTransaction();
	}

	@Override
	public void close() {
		synchronized (CassandraBinaryStorageManager.class) {
			if (null == cassandra)
				logger.warn("Attempted to shutdown Cassandra before startup");
			
			/* deactivate() really just calls stop(), which in turn
			 * stops listening for Thrift connections.  The core
			 * Cassandra database remains loaded and active.
			 * There is no way to completely shut down Cassandra
			 * without killing the JVM as of this writing at 0.7.5.
			 */
//			cassandra.deactivate();
			cassandraStopped = true;
		}
	}
	
	public void dropDatabase(String name) throws GraphStorageException {
		CassandraServer cs = new CassandraServer();
		
		
		try {
			cs.set_keyspace(keyspace);
			KsDef ks = cs.describe_keyspace(keyspace);
			for (CfDef cf : ks.getCf_defs())
				if (cf.getName().equals(name)) {
					cs.system_drop_column_family(name);
					try {
						Thread.sleep(100L);
					} catch (InterruptedException e) {
						throw new GraphStorageException(e);
					}
					return;
				}
		} catch (InvalidRequestException e) {
			throw new GraphStorageException(e);
		} catch (TException e) {
			throw new GraphStorageException(e);
		} catch (NotFoundException e) {
			throw new GraphStorageException(e);
		}

	}
	
	private static CassandraDaemon startDaemon() throws IOException {
        // initialize keyspaces
    	logger.debug("Starting Cassandra");
    	final CassandraDaemon cd = new CassandraDaemon();
    	Thread t = new Thread() { // TODO threadpoolify
    		@Override
    		public void run() {
    	    	cd.activate();
    		}
    	};
    	t.start();
    	logger.debug("Cassandra Started");
    	
        /*
         * init() does not necessarily block until the local Cassandra
         * instance has completely started and converged its state with the cluster.
         * This has resulted in bizarre errors like "No replica strategy
         * configured for <keyspace>" where said keyspace exists and does have
         * a replication strategy configured.
         * 
         * As of 0.7, Cassandra has a builtin sleep during init() of at least
         * five seconds (I think).  It might be longer.  But even this sleep is
         * not always enough.  So we sleep a while longer.  The amount of time
         * needed seems to vary depending on the node loads and number of boxes
         * involved.
         */
    	long sleepMillis = 6000L;
    	logger.debug("Sleeping " + sleepMillis + " ms");
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        logger.debug("Woke up");
        
        return cd;
	}
}
