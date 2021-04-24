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

    private volatile boolean interruptible = true;
    private volatile boolean softInterrupted = false;

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

        /* We use interrupted() instead of isInterrupted() to guarantee that the
         * interrupt flag is cleared when we exit this loop. cleanup() can then
         * run blocking operations without failing due to interruption.
         */
        while (!interrupted() && !softInterrupted) {

            try {
                waitCondition();
            } catch (InterruptedException e) {
                log.debug("Interrupted in background thread wait condition", e);
                break;
            }

            /* This check could be removed without affecting correctness. At
             * worst, removing it should just reduce shutdown responsiveness in
             * a couple of corner cases:
             *
             * 1. Rare interruptions are those that occur while this thread is
             * in the RUNNABLE state
             *
             * 2. Odd waitCondition() implementations that swallow an
             * InterruptedException and set the interrupt status instead of just
             * propagating the InterruptedException to us
             */
            if (interrupted())
                break;

            interruptible = false;
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
                interruptible = true;
            }
        }

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

        if (interruptible)
            interrupt();

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
