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
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.thrift.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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
    private static final Logger log = LoggerFactory.getLogger(CassandraThriftStoreManager.class);

    private final Map<String, CassandraThriftKeyColumnValueStore> openStores;

    private final UncheckedGenericKeyedObjectPool <String, CTConnection> pool;

    public CassandraThriftStoreManager(Configuration config) throws StorageException {
        super(config);

        this.pool = CTConnectionPool.getPool(hostname,
                                             port,
                                             config.getInt(GraphDatabaseConfiguration.CONNECTION_TIMEOUT_KEY,
                                                           GraphDatabaseConfiguration.CONNECTION_TIMEOUT_DEFAULT),
                                             thriftFrameSize,
                                             thriftMaxMessageSize);

        this.openStores = new HashMap<String, CassandraThriftKeyColumnValueStore>();
    }

    @Override
    public Partitioner getPartitioner() throws StorageException {
        return Partitioner.getPartitioner(getCassandraPartitioner());
    }

    public IPartitioner<?> getCassandraPartitioner() throws StorageException {
        CTConnection conn = null;
        try {
            conn = getCassandraConnection();
            return (IPartitioner<?>) Class.forName(conn.getClient().describe_partitioner()).newInstance();
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

    @Override // TODO: *BIG FAT WARNING* 'synchronized is always *bad*, change openStores to use ConcurrentLinkedHashMap
    public synchronized CassandraThriftKeyColumnValueStore openDatabase(final String name) throws StorageException {
        if (openStores.containsKey(name))
            return openStores.get(name);

        ensureColumnFamilyExists(keySpaceName, name);

        CassandraThriftKeyColumnValueStore store = new CassandraThriftKeyColumnValueStore(keySpaceName, name, this, pool);
        openStores.put(name, store);
        return store;
	}


    /**
     * Connect to Cassandra via Thrift on the specified host and port and attempt to truncate the named keyspace.
     *
     * This is a utility method intended mainly for testing. It is
     * equivalent to issuing 'truncate <cf>' for each of the column families in keyspace using
     * the cassandra-cli tool.
     *
     * Using truncate is better for a number of reasons, most significantly because it doesn't
     * involve any schema modifications which can take time to propagate across the cluster such
     * leaves nodes in the inconsistent state and could result in read/write failures.
     * Any schema modifications are discouraged until there is no traffic to Keyspace or ColumnFamilies.
     *
     * @throws StorageException if any checked Thrift or UnknownHostException is thrown in the body of this method
     */
    public void clearStorage() throws StorageException {
        openStores.clear();

        CTConnection conn = null;
        try {
            conn = getCassandraConnection();
            Cassandra.Client client = conn.getClient();

            try {
                client.set_keyspace(keySpaceName);

                KsDef ksDef = client.describe_keyspace(keySpaceName);

                for (CfDef cfDef : ksDef.getCf_defs())
                    client.truncate(cfDef.name);


                CTConnectionPool.clearPool(hostname, port, GraphDatabaseConfiguration.CONNECTION_TIMEOUT_DEFAULT, keySpaceName);
            } catch (InvalidRequestException e) { // Keyspace doesn't exist yet: return immediately
                log.debug("Keyspace {} does not exist, not attempting to truncate.", keySpaceName);
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

    private void ensureColumnFamilyExists(String ksName, String cfName) throws StorageException {
        ensureColumnFamilyExists(ksName, cfName, "org.apache.cassandra.db.marshal.BytesType");
    }

    private void ensureColumnFamilyExists(String ksName, String cfName, String comparator) throws StorageException {
        CTConnection conn = null;
        try {
            KsDef keyspaceDef = ensureKeyspaceExists(ksName);

            conn = pool.genericBorrowObject(ksName);
            Cassandra.Client client = conn.getClient();

            log.debug("Looking up metadata on keyspace {}...", ksName);

            boolean foundColumnFamily = false;
            for (CfDef cfDef : keyspaceDef.getCf_defs()) {
                String curCfName = cfDef.getName();
                if (curCfName.equals(cfName))
                    foundColumnFamily = true;
            }

            if (!foundColumnFamily) {
                createColumnFamily(client, ksName, cfName, comparator);
            } else {
                log.debug("Keyspace {} and ColumnFamily {} were found.", ksName, cfName);
            }
        } catch (SchemaDisagreementException e) {
            throw new TemporaryStorageException(e);
        } catch (Exception e) {
            throw new PermanentStorageException(e);
        } finally {
            IOUtils.closeQuietly(conn);
        }
    }

    private static void createColumnFamily(Cassandra.Client client, String ksName, String cfName) throws StorageException {
        createColumnFamily(client, ksName, cfName, "org.apache.cassandra.db.marshal.BytesType");
    }

    private static void createColumnFamily(Cassandra.Client client,
                                           String ksName,
                                           String cfName,
                                           String comparator) throws StorageException {
        CfDef createColumnFamily = new CfDef();
        createColumnFamily.setName(cfName);
        createColumnFamily.setKeyspace(ksName);
        createColumnFamily.setComparator_type(comparator);

        // Hard-coded caching settings
        if (cfName.startsWith(Backend.EDGESTORE_NAME)) {
            createColumnFamily.setCaching("keys_only");
        } else if (cfName.startsWith(Backend.VERTEXINDEX_STORE_NAME)) {
            createColumnFamily.setCaching("rows_only");
        }

        log.debug("Adding column family {} to keyspace {}...", cfName, ksName);
        String schemaVer;
        try {
            schemaVer = client.system_add_column_family(createColumnFamily);
        } catch (SchemaDisagreementException e) {
            throw new TemporaryStorageException("Error in setting up column family", e);
        } catch (Exception e) {
            throw new PermanentStorageException(e);
        }

        log.debug("Added column family {} to keyspace {}.", cfName, ksName);

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
            ensureColumnFamilyExists(keySpaceName, SYSTEM_PROPERTIES_CF, "org.apache.cassandra.db.marshal.UTF8Type");

            connection = getCassandraConnection();
            Cassandra.Client client = connection.getClient();

            client.set_keyspace(keySpaceName);

            ColumnOrSuperColumn column = client.get(UTF8Type.instance.fromString(SYSTEM_PROPERTIES_KEY),
                                                   new ColumnPath(SYSTEM_PROPERTIES_CF).setColumn(UTF8Type.instance.fromString(key)),
                                                   ConsistencyLevel.QUORUM);

            if (column == null || !column.isSetColumn())
                    return null;

            Column actualColumn = column.getColumn();

            return (actualColumn.value == null)
                    ? null
                    : UTF8Type.instance.getString(actualColumn.value);
        } catch (NotFoundException e) {
                return null;
        } catch (Exception e) {
            throw new PermanentStorageException(e);
        } finally {
            IOUtils.closeQuietly(connection);
        }
    }

    @Override
    public void setConfigurationProperty(final String rawKey, final String rawValue) throws StorageException {
        CTConnection connection = null;

        try {
            ensureColumnFamilyExists(keySpaceName, SYSTEM_PROPERTIES_CF, "org.apache.cassandra.db.marshal.UTF8Type");

            ByteBuffer key = UTF8Type.instance.fromString(rawKey);
            ByteBuffer val = UTF8Type.instance.fromString(rawValue);

            connection = getCassandraConnection();
            Cassandra.Client client = connection.getClient();

            client.set_keyspace(keySpaceName);

            client.insert(UTF8Type.instance.fromString(SYSTEM_PROPERTIES_KEY),
                          new ColumnParent(SYSTEM_PROPERTIES_CF),
                          new Column(key).setValue(val).setTimestamp(System.currentTimeMillis()),
                          ConsistencyLevel.QUORUM);
        } catch (Exception e) {
            throw new PermanentStorageException(e);
        } finally {
            IOUtils.closeQuietly(connection);
        }
    }

    private CTConnection getCassandraConnection() throws TTransportException {
        return CTConnectionPool.getFactory(hostname,
                                           port,
                                           GraphDatabaseConfiguration.CONNECTION_TIMEOUT_DEFAULT,
                                           thriftFrameSize,
                                           thriftMaxMessageSize).makeRawConnection();
    }
}
