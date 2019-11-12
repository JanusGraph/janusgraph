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

package org.janusgraph.core.schema;

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
        return state == State.DONE || state == State.UNKNOWN;
    }

    public boolean hasFailed() {
        return state == State.FAILED;
    }

    public boolean isRunning() {
        return state == State.RUNNING;
    }

    public long getNumProcessed() {
        return numProcessed;
    }

}
