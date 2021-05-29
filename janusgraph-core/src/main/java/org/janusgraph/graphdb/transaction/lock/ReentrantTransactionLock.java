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

package org.janusgraph.graphdb.transaction.lock;

import org.janusgraph.core.JanusGraphException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ReentrantTransactionLock extends ReentrantLock implements TransactionLock {

    private static final long serialVersionUID = -1533050153710486569L;



    private static final Logger log = LoggerFactory.getLogger(ReentrantTransactionLock.class);

    @Override
    public void lock(Duration timeout) {
        boolean success = false;
        try {
            success = super.tryLock(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            log.warn("Interrupted waiting for lock: {}", this, e);
        }
        if (!success) throw new JanusGraphException("Possible dead lock detected. Waited for transaction lock without success");
    }

    @Override
    public boolean inUse() {
        return super.isLocked() || super.hasQueuedThreads();
    }


}
