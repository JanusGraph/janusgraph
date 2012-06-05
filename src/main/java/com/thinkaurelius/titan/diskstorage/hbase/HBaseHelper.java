package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.core.GraphStorageException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class HBaseHelper {

    /**
     * Deletes the specified table with all its columns.
     * ATTENTION: Invoking this method will delete the table if it exists and therefore causes data loss.
     *
     * @param tableName Name of the table to delete
     */
    public static void deleteAll(String tableName) {
        Configuration conf = HBaseConfiguration.create();
        try {
            HBaseAdmin adm = new HBaseAdmin(conf);
            try {
                adm.disableTable(tableName);
            } catch (Exception e) {
                /*
                     * Swallow exception.  Disabling a table typically throws
                     * an exception because the table doesn't exist or is
                     * already disabled.  If there's a serious problem
                     * interacting with HBase, then the following remove
                     * statement will generate an appropriate exception
                     * (which would propagate up as a RuntimeException).
                     */
            }
            adm.deleteTable(tableName);
        } catch (TableNotFoundException e) {
            // Do nothing
        } catch (IOException e) {
            throw new GraphStorageException(e);
        }
    }
    
    public static void deleteAll(org.apache.commons.configuration.Configuration config) {
        deleteAll(config.getString(HBaseStorageManager.TABLE_NAME_KEY,HBaseStorageManager.TABLE_NAME_DEFAULT));
    }
    
}
