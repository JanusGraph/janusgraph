package com.thinkaurelius.titan.graphdb.log;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.log.Log;
import com.thinkaurelius.titan.diskstorage.log.LogManager;
import com.thinkaurelius.titan.diskstorage.log.ReadMarker;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: make sure this keeps it own time anchors in the configuration at the point of the last completely closed transaction
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardTransactionLogProcessor {

    private static final Logger logger =
            LoggerFactory.getLogger(StandardTransactionLogProcessor.class);

    private final StandardTitanGraph graph;
    private final TimestampProvider times;
    private final LogManager txLogManager;
    private final Log txLog;


    public StandardTransactionLogProcessor(StandardTitanGraph graph) {
        Preconditions.checkArgument(graph != null && graph.isOpen());
        this.graph = graph;
        this.times = graph.getConfiguration().getTimestampProvider();
        this.txLogManager = graph.getBackend().getLogManager(GraphDatabaseConfiguration.TRANSACTION_LOG);
        try {
            this.txLog = txLogManager.openLog(Backend.SYSTEM_TX_LOG_NAME, ReadMarker.fromNow());
        } catch (StorageException e) {
            throw new TitanException(e);
        }
    }

    public synchronized void shutdown() throws TitanException {
        if (graph.isOpen())
        try {
            txLog.close();
            txLogManager.close();
        } catch (StorageException e) {
            throw new TitanException(e);
        }
        graph.shutdown();
    }


}
