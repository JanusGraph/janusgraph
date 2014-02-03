package com.thinkaurelius.titan.diskstorage.util;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTimestampProvider implements TimestampProvider {

    private static final Logger log =
            LoggerFactory.getLogger(AbstractTimestampProvider.class);

    @Override
    public long sleepUntil(final long time, final TimeUnit unit, final Logger log) throws InterruptedException {

        // All long variables are times in parameter unit

        long now;

        while ((now = unit.convert(getTime(), getUnit())) <= time) {
            final long delta = time - now;
            /*
             * TimeUnit#sleep(long) internally preserves the nanoseconds parts
             * of the argument, if applicable, and passes both milliseconds and
             * nanoseconds parts into Thread#sleep(long, long)
             */
//            if (log.isDebugEnabled()) {
//                log.error("Sleeping: now={} targettime={} delta={} (unit={})",
//                        new Object[] { now, time, delta, unit });
//            }
            unit.sleep(delta);
        }

        return now;
    }

    @Override
    public long sleepUntil(final long time, final TimeUnit unit) throws InterruptedException {
        return sleepUntil(time, unit, log);
    }

    @Override
    public String toString() {
        return "TimestampProvider[" + getUnit() + "]";
    }
}
