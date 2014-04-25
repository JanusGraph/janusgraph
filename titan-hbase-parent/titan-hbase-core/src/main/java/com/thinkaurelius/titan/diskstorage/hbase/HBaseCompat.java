package com.thinkaurelius.titan.diskstorage.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;

public interface HBaseCompat {
    public void setCompression(HColumnDescriptor cd, String algo);
}
