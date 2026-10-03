// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.util.system;

import org.apache.commons.lang.UnhandledException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.util.datastructures.ExceptionWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecuteUtil {

    private static final Logger log = LoggerFactory.getLogger(ExecuteUtil.class);

    public static void executeWithCatching(ExecuteExceptionallyFunction function, ExceptionWrapper exceptionWrapper){
        try {
            function.execute();
        } catch (Throwable throwable){
            if(exceptionWrapper.getThrowable() == null){
                exceptionWrapper.setThrowable(throwable);
            } else {
                exceptionWrapper.getThrowable().addSuppressed(throwable);
            }
        }
    }

    public static void throwIfException(ExceptionWrapper exceptionWrapper) throws BackendException {
        if(exceptionWrapper.getThrowable() != null){
            Throwable throwable = exceptionWrapper.getThrowable();
            if(throwable instanceof BackendException){
                throw (BackendException) throwable;
            } else if(throwable instanceof RuntimeException){
                throw (RuntimeException) throwable;
            } else {
                throw new UnhandledException(throwable);
            }
        }
    }

    /**
     * Shuts the executor service down and waits up to the given time for its tasks to finish, then interrupts the
     * threads of those which have not. An interrupt of the waiting thread ends the wait at once: the threads are
     * interrupted, and the interrupt is restored before this method returns.
     *
     * @param executorService the executor service to shut down, or null
     * @param maxWaitTime the longest wait in milliseconds
     */
    public static void gracefulExecutorServiceShutdown(ExecutorService executorService, long maxWaitTime){
        if(executorService == null || executorService.isTerminated()){
            return;
        }
        executorService.shutdown();
        try{
            if (executorService.awaitTermination(maxWaitTime, TimeUnit.MILLISECONDS)) {
                return;
            }
            log.warn("ExecutorService {} was not terminated in {} ms. ExecutorService will be terminated by interrupting its managed threads.",
                executorService, maxWaitTime);
        } catch (InterruptedException throwable){
            log.warn("Termination await of the ExecutorService {} was interrupted. ExecutorService will be terminated by interrupting its managed threads.",
                executorService);
            Thread.currentThread().interrupt();
        }
        executorService.shutdownNow();
    }
}
