package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.BerkeleyJeStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class BerkeleyJEGraphTest extends TitanGraphTest {

    public BerkeleyJEGraphTest() {
        super(BerkeleyJeStorageSetup.getBerkeleyJEGraphConfiguration());
        //System.out.println(BerkeleyJeStorageSetup.getHomeDir());
    }

}
