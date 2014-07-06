package com.thinkaurelius.titan.diskstorage.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.Compression;

public class HBaseCompat0_96 implements HBaseCompat {

    @Override
    public void setCompression(HColumnDescriptor cd, String algo) {
        cd.setCompressionType(Compression.Algorithm.valueOf(algo));
    }

    @Override
    public HTableDescriptor newTableDescriptor(String tableName) {
        TableName tn = TableName.valueOf(tableName);
        return new HTableDescriptor(tn);
    }
}
