package com.thinkaurelius.titan.core.log;

import com.thinkaurelius.titan.core.TitanException;

/**
 * Framework for processing transaction logs. Using the {@link LogProcessorBuilder} returned by
 * {@link #addLogProcessor(String)} one can process the change events for a particular transaction log identified by name.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface LogProcessorFramework {

    /**
     * Returns a processor builder for the transaction log with the given log identifier.
     * Only one processor may be registered per transaction log.
     *
     * @param logIdentifier Name that identifies the transaction log to be processed,
     *                      i.e. the one used in {@link com.thinkaurelius.titan.core.TransactionBuilder#logIdentifier(String)}
     * @return
     */
    public LogProcessorBuilder addLogProcessor(String logIdentifier);

    /**
     * Removes the log processor for the given identifier and closes the associated log.
     *
     * @param logIdentifier
     * @return
     */
    public boolean removeLogProcessor(String logIdentifier);

    /**
     * Closes all log processors, their associated logs, and the backing graph instance
     *
     * @throws TitanException
     */
    public void shutdown() throws TitanException;


}
