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

package org.janusgraph.core.log;


import java.time.Instant;

/**
 * Identifies a transaction. Used when processing user log entries to know which transaction caused a given change.
 * A transaction is uniquely identified by the unique identifier of the instance that executed the transaction, the time
 * of the transaction, and an instance local transaction id.
 * <p>
 * Note, that all 3 pieces of information are required for a unique identification of the transaction.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TransactionId {

    /**
     * Returns the unique id of the JanusGraph graph instance which executed the transaction.
     *
     * @return
     */
    String getInstanceId();

    /**
     * Returns the unique transaction id within a particular JanusGraph instance.
     * @return
     */
    long getTransactionId();

    /**
     * Returns the time of the transaction
     *
     * @return
     */
    Instant getTransactionTime();

}
