package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class ExternalHBaseGraphTest extends TitanGraphTest {

    public ExternalHBaseGraphTest() {
        super(StorageSetup.getHBaseGraphConfiguration());
    }

}
