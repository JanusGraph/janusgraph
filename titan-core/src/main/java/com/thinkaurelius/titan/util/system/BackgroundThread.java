package com.thinkaurelius.titan.util.system;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class BackgroundThread extends Thread {

    private static final Logger log =
            LoggerFactory.getLogger(BackgroundThread.class);

    private volatile boolean stop = false;
    private boolean waiting = false;

    /**
     *
     * NEVER set daemon=true and override the cleanup() method. If this is a daemon thread there is no guarantee that
     * cleanup is called.
     *
     * @param name
     * @param daemon
     */
    public BackgroundThread(String name, boolean daemon) {
        this.setName(name + getId());
        this.setDaemon(daemon);
    }

    @Override
    public void run() {
        while (true) {
            try {
                synchronized (this) {
                    if (stop) break;
                    waiting=true;
                }
                waitCondition();
                synchronized (this) {
                    waiting=false;
                }
            } catch (InterruptedException e) {
                if (stop) break;
                else throw new RuntimeException(getName() + " thread got interrupted",e);
            }
            action();
        }
        cleanup();
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
     * This action will not be interrupted.
     */
    protected abstract void action();

    /**
     * Any clean up that needs to be done before this thread is closed down.
     */
    protected void cleanup() {
        //Do nothing by default
    }

    @Override
    public void interrupt() {
        throw new UnsupportedOperationException("Use close() to properly terminate this thread");
    }

    public void close(long maxWaitMs) {
        synchronized (this) {
            stop = true;
            if (waiting) super.interrupt();
        }
        try {
            super.join(maxWaitMs);
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for thread {} to join",e);
        }
        if (super.isAlive()) {
            log.error("Thread {} did not terminate in time [{}]. This could mean that important clean up functions could not be called.",super.getName(),maxWaitMs);
        }
    }

}
