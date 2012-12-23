package com.thinkaurelius.titan.upgrade;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJEKeyValueStore;
import com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJEStoreManager;
import com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJETx;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManagerAdapter;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.IOUtils;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class Upgrade010to020 {
    
    public static void main(String[] args) throws StorageException, IOException {
        if (args.length!=1) throw new IllegalArgumentException("Expecting exactly one argument: The Titan configuration file");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Did you shut down all running Titan instances for this database (yes/no)?");
        String user = in.readLine().trim().toLowerCase();
        if (!user.equals("yes") && !user.equals("y")) throw new IllegalStateException("Please shut down all running Titan instances first!");
        System.out.println();
        System.out.println("Did you make a backup/copy of your Titan database (yes/no)?");
        user = in.readLine().trim().toLowerCase();
        if (!user.equals("yes") && !user.equals("y")) throw new IllegalStateException("Please make a backup first to ensure the safety of your data!");

        File configFile = new File(args[0]);
        Configuration config = null;
        try {
            if (configFile.isFile()) {
                System.out.println("Opening local database at: " + configFile.getAbsoluteFile());
                config = new PropertiesConfiguration(configFile);
            } else if (configFile.isDirectory()) {
                System.out.println("Opening configuration file at: " + configFile.getAbsoluteFile());
                config = new BaseConfiguration();
                config.setProperty(keyInNamespace(STORAGE_NAMESPACE,STORAGE_BACKEND_KEY),"berkeleyje");
                config.setProperty(keyInNamespace(STORAGE_NAMESPACE,STORAGE_DIRECTORY_KEY),configFile.getAbsolutePath());
            } else {
                throw new IllegalArgumentException("Expected configuration file or directory as input.");
            }
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException("Could not load configuration at: " + configFile,e);
        }
        
        if (config.getInt("id-random-bits",0)!=0)
            throw new IllegalStateException("Your database configuration does not allow using this upgrade script. Please contact the mailing list for help.");

        long[] oldvalues = new long[3];

        Configuration storeconfig = config.subset(STORAGE_NAMESPACE);
        String backend = storeconfig.getString(STORAGE_BACKEND_KEY, null);
        if (backend==null) {
            throw new IllegalArgumentException("Storage backend not configured");
        } else if (backend.equalsIgnoreCase("local") || backend.equalsIgnoreCase("berkeleyje")) {
            BerkeleyJEStoreManager store = new BerkeleyJEStoreManager(storeconfig);
            String idManagerTableName = storeconfig.getString("idmanager_table", "titan_idmanager");
            BerkeleyJEKeyValueStore idDB = store.openDatabase(idManagerTableName);
            BerkeleyJEKeyValueStore idDBwrap = store.openDatabase("titan_ids");
            KeyColumnValueStore idDBnew = KeyValueStoreManagerAdapter.wrapKeyValueStore(
                    idDBwrap, Backend.STATIC_KEY_LENGTHS);
            BerkeleyJETx tx = store.beginTransaction(ConsistencyLevel.DEFAULT);

            for (int partition=oldvalues.length-1;partition>=0;partition--) {
                System.out.println("Updating partition: " + partition);
                //Read old
                ByteBuffer oldkey = ByteBufferUtil.getIntByteBuffer(partition);
                ByteBuffer newkey = ByteBufferUtil.getIntByteBuffer(partition+1);

                ByteBuffer value = idDB.get(oldkey,tx);
                Preconditions.checkArgument(value!=null && value.remaining()==4);
                oldvalues[partition] = value.getInt();
                idDB.delete(oldkey,tx);
                //Write new
                idDBnew.mutate(newkey, ImmutableList.of(new Entry(ByteBuffer.allocate(1), ByteBufferUtil.getLongByteBuffer(oldvalues[partition]))), null, tx);
            }
            tx.commit();
        } else if (backend.equalsIgnoreCase("cassandra") || backend.equalsIgnoreCase("cassandrathrift") ||
                backend.equalsIgnoreCase("astyanax") || backend.equalsIgnoreCase("hbase") ||
                backend.equalsIgnoreCase("embeddedcassandra")) {
            Map<String,String> idTableMapper = ImmutableMap.of("hbase","blocks_allocated","embeddedcassandra","titan_ids",
                    "astyanax","titan_ids","cassandrathrift","titan_ids","cassandra","titan_ids");
            
            
            KeyColumnValueStoreManager store = (KeyColumnValueStoreManager)Backend.getStorageManager(storeconfig);
            KeyColumnValueStore idstore = store.openDatabase(idTableMapper.get(backend));
            StoreTransaction tx = store.beginTransaction(ConsistencyLevel.KEY_CONSISTENT);
            final ByteBuffer empty = ByteBuffer.allocate(0);

            long baseBlockSize = storeconfig.getLong("idauthority-block-size",10000);
            System.out.println("The configured id block size is ["+baseBlockSize+"]. Is this correct (yes/no)?");
            user = in.readLine().trim().toLowerCase();
            if (!user.equals("yes") && !user.equals("y")) throw new IllegalStateException("Please configure the correct block size using the old configuration options before proceeding.");

            SimpleVertexIDBlockSizer sizer = new SimpleVertexIDBlockSizer(baseBlockSize);

            for (int partition=oldvalues.length-1;partition>=0;partition--) {
                System.out.println("Updating partition: " + partition);

                ByteBuffer oldkey = ByteBufferUtil.getIntByteBuffer(partition);
                ByteBuffer newkey = ByteBufferUtil.getIntByteBuffer(partition+1);
                //Read old
                List<Entry> blocks = idstore.getSlice(oldkey, empty, empty, tx);
                Preconditions.checkArgument(blocks!=null && !blocks.isEmpty());
                long latest = -1;

                for (Entry e : blocks) {
                    ByteBuffer read = e.getColumn();
                    read.mark();
                    long counterVal = read.getLong();
                    read.reset();
                    if (latest < counterVal) {
                        latest = counterVal;
                    }
                    idstore.mutate(oldkey,null,ImmutableList.of(e.getColumn()),tx);
                }
                Preconditions.checkArgument(latest>0);
                //Update to new id model
                latest = latest+sizer.getBlockSize(partition);
                oldvalues[partition]=latest;
                //Write new
                byte[] rid = DistributedStoreManager.getRid(storeconfig);
                ByteBuffer target = ByteBuffer.allocate(
                        8 // counter long
                                + 8 // time in ms
                                + rid.length);

                target.putLong(-latest).putLong(System.currentTimeMillis()).put(rid);
                target.rewind();
                idstore.mutate(newkey, Arrays.asList(new Entry(target, ByteBuffer.allocate(0))), null, tx);

            }
            tx.commit();
        } else {
            throw new IllegalArgumentException("Storage backend not supported for upgrade ["+backend+"]. Please contact the mailing list.");
        }
        System.out.println("Update successful. Only use Titan 0.2.x from this point on!");
        System.out.println("Updated ID values:");
        for (int i=0;i<oldvalues.length;i++) {
            System.out.println("Partition ["+i+"]: " + oldvalues[i]);
        }
    }

    private static class SimpleVertexIDBlockSizer implements IDBlockSizer {

        private static final int AVG_EDGES_PER_VERTEX = 10;
        private static final int DEFAULT_NUM_EDGE_TYPES = 50;

        private final long baseBlockSize;

        SimpleVertexIDBlockSizer(final long size) {
            Preconditions.checkArgument(size>0 && size<Integer.MAX_VALUE);
            this.baseBlockSize=size;
        }

        @Override
        public long getBlockSize(int partitionID) {
            if (IDManager.IDType.Node.is(partitionID)) {
                return baseBlockSize;
            } else if (IDManager.IDType.Edge.is(partitionID)) {
                return baseBlockSize*AVG_EDGES_PER_VERTEX;
            } else if (IDManager.IDType.EdgeType.is(partitionID)) {
                return DEFAULT_NUM_EDGE_TYPES;
            } else throw new IllegalArgumentException("Invalid partition id: " + partitionID);
        }
    }
    
}
