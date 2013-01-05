package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class ExternalHBaseGraphTest extends TitanGraphTest {

    public ExternalHBaseGraphTest() {
        super(HBaseStorageSetup.getHBaseGraphConfiguration());
    }

}
