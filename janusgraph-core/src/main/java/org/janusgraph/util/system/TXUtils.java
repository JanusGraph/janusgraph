package org.janusgraph.util.system;

import org.janusgraph.core.JanusGraphTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class may become obsolete in and is subject for removal in JanusGraph 0.9,
 * since we can assume try-with-resources and transactions implement AutoCloseable.
 */
public class TXUtils {

    private static final Logger log =
            LoggerFactory.getLogger(TXUtils.class);

    public static void rollbackQuietly(JanusGraphTransaction tx) {
        if (null == tx)
            return;

        try {
            tx.rollback();
        } catch (Throwable t) {
            log.warn("Unable to rollback transaction", t);
        }
    }
}
