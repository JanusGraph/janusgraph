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

package org.janusgraph.util.system;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class BackgroundThread extends Thread {

    private static final Logger log =
            LoggerFactory.getLogger(BackgroundThread.class);

    //Read and written only under interruptLock
    private boolean interruptible = true;
    private volatile boolean softInterrupted = false;
    //close() interrupts only while it holds this lock and the thread is interruptible, and the thread changes
    //interruptible only while holding it, so that once the thread has turned interruptible off no interrupt of
    //close() can reach it any more
    private final Object interruptLock = new Object();

    /**
     *
     * NEVER set daemon=true and override the cleanup() method. If this is a daemon thread there is no guarantee that
     * cleanup is called.
     *
     * @param name
     * @param daemon
     */
    public BackgroundThread(String name, boolean daemon) {
        this.setName(name + ":" + getId());
        this.setDaemon(daemon);
    }

    @Override
    public void run() {

        /* interrupted() clears the flag as it checks it; an interrupt which
         * lands later is cleared below, before cleanup().
         */
        while (!interrupted() && !softInterrupted) {

            try {
                waitCondition();
            } catch (InterruptedException e) {
                log.debug("Interrupted in background thread wait condition", e);
                break;
            }

            /* An interrupt of close() which arrived after the check above, or
             * one which waitCondition() swallowed and set again, is seen here,
             * and keeps action() from running interrupted: close() cannot
             * interrupt the thread between this check and interruptible being
             * turned off.
             */
            synchronized (interruptLock) {
                if (interrupted())
                    break;
                interruptible = false;
            }
            try {
                action();
            } catch (Throwable e) {
                log.error("Exception while executing action on background thread",e);
            } finally {
                /*
                 * This doesn't really need to be in a finally block as long as
                 * we catch Throwable, but it's here as future-proofing in case
                 * the catch-clause type is narrowed in future revisions.
                 */
                synchronized (interruptLock) {
                    interruptible = true;
                }
            }
        }

        /* The loop can end on softInterrupted after close() has decided to
         * interrupt this thread but before the interrupt landed. Turning
         * interruptible off stops any further interrupt of close(), and
         * clearing the flag afterwards drops one which landed before, so that
         * cleanup() can run blocking operations - such as flushing messages
         * through a storage backend - without failing due to interruption.
         */
        synchronized (interruptLock) {
            interruptible = false;
        }
        interrupted();
        try {
            cleanup();
        } catch (Throwable e) {
            log.error("Exception while executing cleanup on background thread",e);
        }

    }

    /**
     * The wait condition for the background thread. This determines what this background thread is waiting for in
     * its execution. This might be elapsing time or availability of resources.
     *
     * Since there is a wait involved, this method should throw an InterruptedException
     *
     * @throws InterruptedException
     */
    protected abstract void waitCondition() throws InterruptedException;

    /**
     * The action taken by this background thread when the wait condition is met.
     * This action should execute swiftly to ensure that this thread can be closed in a reasonable amount of time.
     *
     * This action will not be interrupted by {@link #close(Duration)}.
     */
    protected abstract void action();

    /**
     * Any clean up that needs to be done before this thread is closed down.
     */
    protected void cleanup() {
        //Do nothing by default
    }

    public void close(Duration duration) {

        if (!isAlive()) {
            log.warn("Already closed: {}", this);
            return;
        }

        final long maxWaitMs = duration.toMillis();

        softInterrupted = true;

        synchronized (interruptLock) {
            if (interruptible)
                interrupt();
        }

        try {
            join(maxWaitMs);
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for thread {} to join", getName(), e);
        }
        if (isAlive()) {
            log.error("Thread {} did not terminate in time [{}]. This could mean that important clean up functions could not be called.", getName(), maxWaitMs);
        }
    }

}
