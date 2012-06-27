package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.graphdb.TitanGraphTest;

import com.thinkaurelius.titan.StorageSetup;

public class ExternalHBaseGraphTest extends TitanGraphTest {

	public ExternalHBaseGraphTest() {
		super(StorageSetup.getHBaseGraphConfiguration());
	}

}
