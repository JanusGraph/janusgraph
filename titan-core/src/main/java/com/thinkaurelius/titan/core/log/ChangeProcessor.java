package com.thinkaurelius.titan.core.log;

import com.thinkaurelius.titan.core.TitanTransaction;

/**
 * Allows the user to define custom behavior to process those transactional changes that are recorded in a transaction log.
 * {@link ChangeProcessor}s are registered with a transaction log processor in the {@link LogProcessorBuilder}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ChangeProcessor {

    /**
     * Process the changes caused by the transaction identified by {@code txId} within a newly opened transaction {@code tx}.
     * The changes are captured in the {@link ChangeState} data structure.
     *
     * @param tx
     * @param txId
     * @param changeState
     */
    public void process(TitanTransaction tx, TransactionId txId, ChangeState changeState);

}
