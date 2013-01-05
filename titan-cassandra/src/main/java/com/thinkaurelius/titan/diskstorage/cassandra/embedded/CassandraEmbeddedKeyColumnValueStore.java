package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Mutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;

public class CassandraEmbeddedKeyColumnValueStore
        implements KeyColumnValueStore {

    private static final Logger log = LoggerFactory
            .getLogger(CassandraEmbeddedKeyColumnValueStore.class);

    private final String keyspace;
    private final String columnFamily;
    private final CassandraEmbeddedStoreManager storeManager;


    public CassandraEmbeddedKeyColumnValueStore(
            String keyspace,
            String columnFamily,
            CassandraEmbeddedStoreManager storeManager) throws RuntimeException {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.storeManager = storeManager;
    }

    @Override
    public void close() throws StorageException {
    }

    @Override
    public ByteBuffer get(ByteBuffer key, ByteBuffer column,
                          StoreTransaction txh) throws StorageException {

        QueryPath slicePath = new QueryPath(columnFamily);

        SliceByNamesReadCommand namesCmd = new SliceByNamesReadCommand(
                keyspace, key.duplicate(), slicePath, Arrays.asList(column.duplicate()));

        List<Row> rows = read(namesCmd, getTx(txh).getReadConsistencyLevel().getDBConsistency());

        if (null == rows || 0 == rows.size())
            return null;

        if (1 < rows.size())
            throw new PermanentStorageException("Received " + rows.size()
                    + " rows from a single-key-column cassandra read");

        assert 1 == rows.size();

        Row r = rows.get(0);

        if (null == r) {
            log.warn("Null Row object retrieved from Cassandra StorageProxy");
            return null;
        }

        ColumnFamily cf = r.cf;
        if (null == cf)
            return null;

        if (cf.isMarkedForDelete())
            return null;

        IColumn c = cf.getColumn(column.duplicate());
        if (null == c)
            return null;

        // These came up during testing
        if (c.isMarkedForDelete())
            return null;

        return org.apache.cassandra.utils.ByteBufferUtil.clone(c.value());
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column,
                                     StoreTransaction txh) throws StorageException {
        return null != get(key, column, txh);
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column,
                            ByteBuffer expectedValue, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
        return storeManager.getLocalKeyPartition();
    }


    @Override
    public String getName() {
        return columnFamily;
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {

        QueryPath slicePath = new QueryPath(columnFamily);
        ReadCommand sliceCmd = new SliceFromReadCommand(
                keyspace,          // Keyspace name
                key.duplicate(),   // Row key
                slicePath,         // ColumnFamily
                ByteBufferUtil.EMPTY_BYTE_BUFFER, // Start column name (empty means begin at first result)
                ByteBufferUtil.EMPTY_BYTE_BUFFER, // End column name (empty means max out the count)
                false,             // Reverse results? (false=no)
                1);                // Max count of Columns to return

        List<Row> rows = read(sliceCmd, getTx(txh).getReadConsistencyLevel().getDBConsistency());

        if (null == rows || 0 == rows.size())
            return false;
        
        /*
         * Find at least one live column
		 * 
		 * Note that the rows list may contain arbitrarily many
		 * marked-for-delete elements. Therefore, we can't assume that we're
		 * dealing with a singleton even though we set the maximum column count
		 * to 1.
		 */
        for (Row r : rows) {
            if (null == r || null == r.cf)
                continue;

            if (r.cf.isMarkedForDelete())
                continue;

            for (IColumn ic : r.cf)
                if (!ic.isMarkedForDelete())
                    return true;
        }

        return false;
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
                                ByteBuffer columnEnd, int limit, StoreTransaction txh) throws StorageException {

        QueryPath slicePath = new QueryPath(columnFamily);
        ReadCommand sliceCmd = new SliceFromReadCommand(
                keyspace,        // Keyspace name
                key.duplicate(), // Row key
                slicePath,       // ColumnFamily
                columnStart.duplicate(),     // Start column name (empty means begin at first result)
                columnEnd.duplicate(),       // End column name (empty means max out the count)
                false,           // Reverse results? (false=no)
                limit);          // Max count of Columns to return

        List<Row> slice = read(sliceCmd, getTx(txh).getReadConsistencyLevel().getDBConsistency());

        if (null == slice || 0 == slice.size())
            return new ArrayList<Entry>(0);

        int sliceSize = slice.size();
        if (1 < sliceSize)
            throw new PermanentStorageException("Received " + sliceSize + " rows for single key");

        Row r = slice.get(0);

        if (null == r) {
            log.warn("Null Row object retrieved from Cassandra StorageProxy");
            return new ArrayList<Entry>(0);
        }

        ColumnFamily cf = r.cf;

        if (null == cf) {
            log.warn("null ColumnFamily (\"{}\")", columnFamily);
            return new ArrayList<Entry>(0);
        }

        if (cf.isMarkedForDelete())
            return new ArrayList<Entry>(0);

        return cfToEntries(cf, columnStart, columnEnd);
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
                                ByteBuffer columnEnd, StoreTransaction txh) throws StorageException {
        return getSlice(key, columnStart, columnEnd, Integer.MAX_VALUE, txh);
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions,
                       List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException {
        Map<ByteBuffer, Mutation> mutations = ImmutableMap.of(key, new
                Mutation(additions, deletions));
        mutateMany(mutations, txh);
    }


    public void mutateMany(Map<ByteBuffer, Mutation> mutations,
                           StoreTransaction txh) throws StorageException {
        storeManager.mutateMany(ImmutableMap.of(columnFamily, mutations), txh);
    }

    private List<Row> read(ReadCommand cmd, org.apache.cassandra.db.ConsistencyLevel clvl) throws StorageException {
        ArrayList<ReadCommand> cmdHolder = new ArrayList<ReadCommand>(1);
        cmdHolder.add(cmd);
        return read(cmdHolder, clvl);
    }

    private List<Row> read(List<ReadCommand> cmds, org.apache.cassandra.db.ConsistencyLevel clvl) throws StorageException {
        try {
            return StorageProxy.read(cmds, clvl);
        } catch (IOException e) {
            throw new PermanentStorageException(e);
        } catch (UnavailableException e) {
            throw new TemporaryStorageException(e);
        } catch (RequestTimeoutException e) {
            throw new PermanentStorageException(e);
        } catch (IsBootstrappingException e) {
            throw new TemporaryStorageException(e);
        }
    }


    private List<Entry> cfToEntries(ColumnFamily cf, ByteBuffer columnStart,
                                    ByteBuffer columnEnd) throws StorageException {

        assert !cf.isMarkedForDelete();

        // Estimate size of Entry list, ignoring deleted columns
        int resultSize = 0;
        for (ByteBuffer col : cf.getColumnNames()) {
            IColumn icol = cf.getColumn(col);
            if (null == icol)
                throw new PermanentStorageException("Unexpected null IColumn");

            if (icol.isMarkedForDelete())
                continue;

            resultSize++;
        }

        // Instantiate return collection
        List<Entry> result = new ArrayList<Entry>(resultSize);

        // Populate Entries into return collection
        for (ByteBuffer col : cf.getColumnNames()) {

            IColumn icol = cf.getColumn(col);
            if (null == icol)
                throw new PermanentStorageException("Unexpected null IColumn");

            if (icol.isMarkedForDelete())
                continue;

            ByteBuffer name = org.apache.cassandra.utils.ByteBufferUtil.clone(icol.name());
            ByteBuffer value = org.apache.cassandra.utils.ByteBufferUtil.clone(icol.value());

            if (columnEnd.equals(name))
                continue;

            result.add(new Entry(name, value));
        }

        return result;
    }


}
