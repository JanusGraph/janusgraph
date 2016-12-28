package com.thinkaurelius.titan.core.schema;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JobStatus {

    public enum State { UNKNOWN, RUNNING, DONE, FAILED }

    private final State state;
    private final long numProcessed;

    public JobStatus(State state, long numProcessed) {
        this.state = state;
        this.numProcessed = numProcessed;
    }

    public State getState() {
        return state;
    }

    public boolean isDone() {
        return state==State.DONE || state==State.UNKNOWN;
    }

    public boolean hasFailed() {
        return state==State.FAILED;
    }

    public boolean isRunning() {
        return state==State.RUNNING;
    }

    public long getNumProcessed() {
        return numProcessed;
    }

}
