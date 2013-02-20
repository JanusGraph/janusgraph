package com.thinkaurelius.titan.graphdb.internal;

import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;

public interface InternalElement extends TitanElement {

    public InternalElement it();

    public StandardTitanTx tx();

    public void setID(long id);

    public byte getLifeCycle();

}
