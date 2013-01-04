package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnectionFactory;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnectionPool;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.UncheckedGenericKeyedObjectPool;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Mutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;

/**
 * This class creates {@see CassandraThriftKeyColumnValueStore}s and
 * handles Cassandra-backed allocation of vertex IDs for Titan (when so
 * configured).
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class CassandraThriftStoreManager extends AbstractCassandraStoreManager {

    private static final Logger log =
            LoggerFactory.getLogger(CassandraThriftStoreManager.class);

    private final Map<String, CassandraThriftKeyColumnValueStore> openStores;

    private final UncheckedGenericKeyedObjectPool
            <String, CTConnection> pool;


    public CassandraThriftStoreManager(Configuration config) throws StorageException {
        super(config);

        this.pool = CTConnectionPool.getPool(
                hostname,
                port,
                config.getInt(GraphDatabaseConfiguration.CONNECTION_TIMEOUT_KEY, GraphDatabaseConfiguration.CONNECTION_TIMEOUT_DEFAULT));

        this.openStores = new HashMap<String, CassandraThriftKeyColumnValueStore>();


    }

    @Override
    public Partitioner getPartitioner() throws StorageException {
        CTConnectionFactory fac =
                CTConnectionPool.getFactory(hostname, port, GraphDatabaseConfiguration.CONNECTION_TIMEOUT_DEFAULT);
        CTConnection conn = null;
        try {
            conn = fac.makeRawConnection();
            Cassandra.Client client = conn.getClient();
            String partitioner = client.describe_partitioner();
            return Partitioner.getPartitioner(partitioner);
        } catch (Exception e) {
            throw new TemporaryStorageException(e);
        } finally {
            IOUtils.closeQuietly(conn);
        }
    }


    @Override
    public String toString() {
        return "thriftCassandra" + super.toString();
    }

    @Override
    public void close() throws StorageException {
        openStores.clear();
        //Do NOT close pool as this may cause subsequent pool operations to fail!
    }

    @Override
    public void mutateMany(Map<String, Map<ByteBuffer, Mutation>> mutations, StoreTransaction txh) throws StorageException {
        Preconditions.checkNotNull(mutations);

        long deletionTimestamp = TimeUtility.getApproxNSSinceEpoch(false);
        long additionTimestamp = TimeUtility.getApproxNSSinceEpoch(true);

        ConsistencyLevel consistency = getTx(txh).getWriteConsistencyLevel().getThriftConsistency();

        // Generate Thrift-compatible batch_mutate() datastructure
        // key -> cf -> cassmutation
        int size = 0;
        for (Map<ByteBuffer, Mutation> mutation : mutations.values()) size += mutation.size();
        Map<ByteBuffer, Map<String, List<org.apache.cassandra.thrift.Mutation>>> batch =
                new HashMap<ByteBuffer, Map<String, List<org.apache.cassandra.thrift.Mutation>>>(size);


        for (Map.Entry<String, Map<ByteBuffer, Mutation>> keyMutation : mutations.entrySet()) {
            String columnFamily = keyMutation.getKey();
            for (Map.Entry<ByteBuffer, Mutation> mutEntry : keyMutation.getValue().entrySet()) {
                ByteBuffer key = mutEntry.getKey();

                Map<String, List<org.apache.cassandra.thrift.Mutation>> cfmutation = batch.get(key);
                if (cfmutation == null) {
                    cfmutation = new HashMap<String, List<org.apache.cassandra.thrift.Mutation>>(3);
                    batch.put(key, cfmutation);
                }

                Mutation mutation = mutEntry.getValue();
                List<org.apache.cassandra.thrift.Mutation> thriftMutation = new ArrayList<org.apache.cassandra.thrift.Mutation>(mutations.size());

                if (mutation.hasDeletions()) {
                    for (ByteBuffer buf : mutation.getDeletions()) {
                        Deletion d = new Deletion();
                        SlicePredicate sp = new SlicePredicate();
                        sp.addToColumn_names(buf);
                        d.setPredicate(sp);
                        d.setTimestamp(deletionTimestamp);
                        org.apache.cassandra.thrift.Mutation m = new org.apache.cassandra.thrift.Mutation();
                        m.setDeletion(d);
                        thriftMutation.add(m);
                    }
                }

                if (mutation.hasAdditions()) {
                    for (Entry ent : mutation.getAdditions()) {
                        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
                        Column column = new Column(ent.getColumn());
                        column.setValue(ent.getValue());
                        column.setTimestamp(additionTimestamp);
                        cosc.setColumn(column);
                        org.apache.cassandra.thrift.Mutation m = new org.apache.cassandra.thrift.Mutation();
                        m.setColumn_or_supercolumn(cosc);
                        thriftMutation.add(m);
                    }
                }

                cfmutation.put(columnFamily, thriftMutation);
            }
        }

        CTConnection conn = null;
        try {
            conn = pool.genericBorrowObject(keySpaceName);
            Cassandra.Client client = conn.getClient();

            client.batch_mutate(batch, consistency);
        } catch (Exception ex) {
            throw CassandraThriftKeyColumnValueStore.convertException(ex);
        } finally {
            if (null != conn)
                pool.genericReturnObject(keySpaceName, conn);
        }

    }

    @Override
    public synchronized CassandraThriftKeyColumnValueStore openDatabase(final String name)
            throws StorageException {

        if (openStores.containsKey(name)) return openStores.get(name);
        else {
            CTConnection conn = null;
            try {
                KsDef keyspaceDef = ensureKeyspaceExists(keySpaceName);

                conn = pool.genericBorrowObject(keySpaceName);
                Cassandra.Client client = conn.getClient();
                log.debug("Looking up metadata on keyspace {}...", keySpaceName);
                boolean foundColumnFamily = false;
                for (CfDef cfDef : keyspaceDef.getCf_defs()) {
                    String curCfName = cfDef.getName();
                    if (curCfName.equals(name)) {
                        foundColumnFamily = true;
                    }
                }
                if (!foundColumnFamily) {
                    log.debug("Keyspace {} not found, about to create it", keySpaceName);
                    createColumnFamily(client, keySpaceName, name);
                } else {
                    log.debug("Found keyspace: {}", keySpaceName);
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
                    pool.genericReturnObject(keySpaceName, conn);
            }

            CassandraThriftKeyColumnValueStore store = new CassandraThriftKeyColumnValueStore(keySpaceName, name, this, pool);
            openStores.put(name, store);
            return store;
        }

    }


    /**
     * Connect to Cassandra via Thrift on the specified host and
     * port and attempt to drop the named keyspace.
     * <p/>
     * This is a utility method intended mainly for testing.  It is
     * equivalent to issuing "drop keyspace {@code <keyspace>};" in
     * the cassandra-cli tool.
     *
     * @throws RuntimeException if any checked Thrift or UnknownHostException
     *                          is thrown in the body of this method
     */
    public void clearStorage() throws StorageException {
        openStores.clear();

        CTConnection conn = null;
        try {
            conn = CTConnectionPool.getFactory(hostname, port, GraphDatabaseConfiguration.CONNECTION_TIMEOUT_DEFAULT).makeRawConnection();
            Cassandra.Client client = conn.getClient();
            try {
                CTConnectionPool.getPool(hostname, port, GraphDatabaseConfiguration.CONNECTION_TIMEOUT_DEFAULT).clear(keySpaceName);
                client.describe_keyspace(keySpaceName);
                // Keyspace must exist
                log.debug("Dropping keyspace {}...", keySpaceName);
                String schemaVer = client.system_drop_keyspace(keySpaceName);

                // Try to let Cassandra converge on the new column family
                CTConnectionFactory.validateSchemaIsSettled(client, schemaVer);

            } catch (NotFoundException e) {
                // Keyspace doesn't exist yet: return immediately
                log.debug("Keyspace {} does not exist, not attempting to drop", keySpaceName);
            }
        } catch (Exception e) {
            throw new TemporaryStorageException(e);
        } finally {
            IOUtils.closeQuietly(conn);
        }
    }

    private KsDef ensureKeyspaceExists(String keyspaceName)
            throws NotFoundException, InvalidRequestException, TException,
            SchemaDisagreementException, StorageException {

        CTConnection connection = getCassandraConnection();

        Preconditions.checkNotNull(connection);

        try {
            Cassandra.Client client = connection.getClient();

            try {
                client.set_keyspace(keyspaceName);
                log.debug("Found existing keyspace {}", keyspaceName);
            } catch (InvalidRequestException e) {
                // Keyspace didn't exist; create it
                log.debug("Creating keyspace {}...", keyspaceName);

                KsDef ksdef = new KsDef().setName(keyspaceName)
                        .setCf_defs(new LinkedList<CfDef>()) // cannot be null but can be empty
                        .setStrategy_class("org.apache.cassandra.locator.SimpleStrategy")
                        .setStrategy_options(new HashMap<String, String>() {{
                            put("replication_factor", String.valueOf(replicationFactor));
                        }});

                String schemaVer = client.system_add_keyspace(ksdef);

                // Try to block until Cassandra converges on the new keyspace
                try {
                    CTConnectionFactory.validateSchemaIsSettled(client, schemaVer);
                } catch (InterruptedException ie) {
                    throw new TemporaryStorageException(ie);
                }
            }

            return client.describe_keyspace(keyspaceName);

        } finally {
            IOUtils.closeQuietly(connection);
        }
    }

    private static void createColumnFamily(Cassandra.Client client, String keyspaceName, String columnfamilyName)
            throws InvalidRequestException, TException, StorageException {
        CfDef createColumnFamily = new CfDef();
        createColumnFamily.setName(columnfamilyName);
        createColumnFamily.setKeyspace(keyspaceName);
        createColumnFamily.setComparator_type("org.apache.cassandra.db.marshal.BytesType");

        // Hard-coded caching settings
        if (columnfamilyName.startsWith(Backend.EDGESTORE_NAME)) {
            createColumnFamily.setCaching("keys_only");
        } else if (columnfamilyName.startsWith(Backend.VERTEXINDEX_STORE_NAME)) {
            createColumnFamily.setCaching("rows_only");
        }

        log.debug("Adding column family {} to keyspace {}...", columnfamilyName, keyspaceName);
        String schemaVer = null;
        try {
            schemaVer = client.system_add_column_family(createColumnFamily);
        } catch (SchemaDisagreementException e) {
            throw new TemporaryStorageException("Error in setting up column family", e);
        }
        log.debug("Added column family {} to keyspace {}.", columnfamilyName, keyspaceName);

        // Try to let Cassandra converge on the new column family
        try {
            CTConnectionFactory.validateSchemaIsSettled(client, schemaVer);
        } catch (InterruptedException e) {
            throw new TemporaryStorageException(e);
        }

    }

    @Override
    public String getConfigurationProperty(final String key) throws StorageException {
        CTConnection connection = null;

        try {
            connection = getCassandraConnection();

            // if keyspace doesn't exist or there was a connection problem this will throw an exception different from NPE
            return connection.getClient().describe_keyspace(keySpaceName).getStrategy_options().get(key);
        } catch (Exception e) {
            throw new PermanentStorageException(e);
        } finally {
            IOUtils.closeQuietly(connection);
        }
    }

    @Override
    public void setConfigurationProperty(final String key, final String value) throws StorageException {
        CTConnection connection = null;

        try {
            connection = getCassandraConnection();
            Cassandra.Client client = connection.getClient();
            KsDef ksDef = client.describe_keyspace(keySpaceName); // would throw NotFoundException so no need to assert

            client.system_update_keyspace(new KsDef().setName(ksDef.name)
                    .setDurable_writes(ksDef.durable_writes)
                    .setStrategy_class(ksDef.strategy_class)
                    .setCf_defs(Collections.EMPTY_LIST)
                    .setStrategy_options(new HashMap<String, String>(ksDef.strategy_options) {{
                        put(key, value);
                    }}));
        } catch (Exception e) {
            throw new PermanentStorageException(e);
        } finally {
            IOUtils.closeQuietly(connection);
        }
    }

    private CTConnection getCassandraConnection() throws TTransportException {
        CTConnectionFactory fac = CTConnectionPool.getFactory(hostname,
                port,
                GraphDatabaseConfiguration.CONNECTION_TIMEOUT_DEFAULT);

        return fac.makeRawConnection();
    }
}
