package com.thinkaurelius.titan.blueprints.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.GraphTransaction;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TransactionWrapper {
    
    private final GraphTransaction tx;
    private final int rollingCommitThreshold;
    private int numOperations;

    public TransactionWrapper(final GraphTransaction tx, int bufferSize) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkArgument(bufferSize>=0);
        this.tx=tx;
        this.rollingCommitThreshold=bufferSize;
        this.numOperations=0;
    }

    public void operation() {
        numOperations++;
        if (rollingCommitThreshold>0 && numOperations%rollingCommitThreshold==0) {
            tx.rollingCommit();
            numOperations=0;
        }
    }
    
    public int getCurrentBufferSize() {
        return numOperations;
    }

    public GraphTransaction getTransaction() {
        return tx;
    }
    
}
