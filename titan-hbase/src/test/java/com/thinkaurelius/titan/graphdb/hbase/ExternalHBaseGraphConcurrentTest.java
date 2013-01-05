package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class ExternalHBaseGraphConcurrentTest extends TitanGraphConcurrentTest {

    public ExternalHBaseGraphConcurrentTest() {
        super(HBaseStorageSetup.getHBaseGraphConfiguration());
    }


}
