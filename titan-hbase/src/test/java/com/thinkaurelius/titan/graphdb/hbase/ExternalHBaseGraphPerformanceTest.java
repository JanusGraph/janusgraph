package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

public class ExternalHBaseGraphPerformanceTest extends TitanGraphPerformanceTest {

    public ExternalHBaseGraphPerformanceTest() {
        super(HBaseStorageSetup.getHBaseGraphConfiguration(), 0, 1, false);
    }


}
