package com.thinkaurelius.titan.graphdb.internal;

import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;

/**
 * Internal Element interface adding methods that should only be used by Titan
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalElement extends TitanElement {

    /**
     * Returns this element in the context of the current transaction.
     *
     * @return
     */
    public InternalElement it();

    /**
     * Returns the transaction to which the element is currently bound or should be refreshed into
     * @return
     */
    public StandardTitanTx tx();

    public void setID(long id);

    /**
     * @see ElementLifeCycle
     * @return The lifecycle of this element
     */
    public byte getLifeCycle();

}
