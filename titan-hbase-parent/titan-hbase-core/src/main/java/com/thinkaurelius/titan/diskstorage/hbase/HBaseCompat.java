package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;

public interface HBaseCompat {

    /**
     * Configure the compression scheme {@code algo} on a column family
     * descriptor {@code cd}. The {@code algo} parameter is a string value
     * corresponding to one of the values of HBase's Compression enum. The
     * Compression enum has moved between packages as HBase has evolved, which
     * is why this method has a String argument in the signature instead of the
     * enum itself.
     *
     * @param cd
     *            column family to configure
     * @param algo
     *            compression type to use
     */
    public void setCompression(HColumnDescriptor cd, String algo);

    /**
     * Create and return a HTableDescriptor instance with the given name. The
     * constructors on this method have remained stable over HBase development
     * so far, but the old HTableDescriptor(String) constructor & byte[] friends
     * are now marked deprecated and may eventually be removed in favor of the
     * HTableDescriptor(TableName) constructor. That constructor (and the
     * TableName type) only exists in newer HBase versions. Hence this method.
     *
     * @param tableName
     *            HBase table name
     * @return a new table descriptor instance
     */
    public HTableDescriptor newTableDescriptor(String tableName);

    ConnectionMask createConnection(Configuration conf) throws IOException;

    void addColumnFamilyToTableDescriptor(HTableDescriptor tdesc, HColumnDescriptor cdesc);

    void setTimestamp(Delete d, long timestamp);
}
