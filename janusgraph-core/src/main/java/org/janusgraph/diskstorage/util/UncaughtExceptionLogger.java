// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;

public class UncaughtExceptionLogger implements UncaughtExceptionHandler {

    private static final Logger log =
            LoggerFactory.getLogger(UncaughtExceptionHandler.class);

    /*
     * I don't like duplicating a subset of org.slf4j.Level, but the slf4j API
     * as of 1.7.5 provides no general Logger.log(Level, String, Object...)
     * method. I can't seem to meta-program around this.
     */
    public enum UELevel implements UELogLevel {
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
        }
    }

    private final UELevel level;

    public UncaughtExceptionLogger(UELevel level) {
        this.level = level;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        level.dispatch("Uncaught exception in thread " + t, e);
    }

    interface UELogLevel {
        void dispatch(String message, Throwable t);
    }
}

