package org.janusgraph.graphdb.internal;

import org.janusgraph.core.JanusElement;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusTransaction;
import org.janusgraph.graphdb.transaction.StandardJanusTx;

/**
 * Internal Element interface adding methods that should only be used by Janus
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalElement extends JanusElement {

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
    public StandardJanusTx tx();


    @Override
    public default JanusTransaction graph() {
        return tx();
    }

    public void setId(long id);

    /**
     * @see ElementLifeCycle
     * @return The lifecycle of this element
     */
    public byte getLifeCycle();

    /**
     * Whether this element is invisible and should only be returned to queries that explicitly ask for invisible elements.
     * @return
     */
    public boolean isInvisible();

}
