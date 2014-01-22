package com.thinkaurelius.titan.diskstorage.util;

import java.lang.Thread.UncaughtExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UncaughtExceptionLogger implements UncaughtExceptionHandler {

    private static final Logger log =
            LoggerFactory.getLogger(UncaughtExceptionHandler.class);

    /*
     * I don't like duplicating a subset of org.slf4j.Level, but the slf4j API
     * as of 1.7.5 provides no general Logger.log(Level, String, Object...)
     * method. I can't seem to metaprogram around this.
     */
    public static enum UELevel implements UELogLevel {
        TRACE {
            public void dispatch(String message, Throwable t) {
                log.trace(message, t);
            }
        },
        DEBUG {
            public void dispatch(String message, Throwable t) {
                log.debug(message, t);
            }
        },
        INFO {
            public void dispatch(String message, Throwable t) {
                log.info(message, t);
            }
        },
        WARN {
            public void dispatch(String message, Throwable t) {
                log.warn(message, t);
            }
        },
        ERROR {
            public void dispatch(String message, Throwable t) {
                log.error(message, t);
            }
        };
    }

    private final UELevel level;

    public UncaughtExceptionLogger(UELevel level) {
        this.level = level;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        level.dispatch(String.format("Uncaught exception in thread " + t), e);
    }
}

interface UELogLevel {
    public void dispatch(String message, Throwable t);
}