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

package org.janusgraph.diskstorage;

import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.util.time.TimestampProvider;

import java.time.Instant;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Dan LaRocque (dalaro@hopcount.org)
 */
public interface BaseTransactionConfig {

    /**
     * Returns the commit time of this transaction which is either a custom timestamp provided
     * by the user, the commit time as set by the enclosing operation, or the first time this method is called.
     *
     * @return commit timestamp for this transaction
     */
    Instant getCommitTime();

    /**
     * Sets the commit time of this transaction. If a commit time has already been set, this method throws
     * an exception. Use {@link #hasCommitTime()} to check prior to setting.
     *
     * @param time
     */
    void setCommitTime(Instant time);

    /**
     * Returns true if a commit time has been set on this transaction.
     *
     * @return
     */
    boolean hasCommitTime();

    /**
     * Returns the timestamp provider of this transaction.
     */
    TimestampProvider getTimestampProvider();

    /**
     * Returns the (possibly null) group name for this transaction.
     * Transactions are grouped under this name for reporting and error tracking purposes.
     *
     * @return group name prefix string or null
     */
    String getGroupName();

    /**
     * True when {@link #getGroupName()} is non-null, false when null.
     */
    boolean hasGroupName();

    /**
     * Get an arbitrary transaction-specific option.
     *
     * @param opt option for which to return a value
     * @return value of the option
     */
    <V> V getCustomOption(ConfigOption<V> opt);

    /**
     * Return any transaction-specific options.
     *
     * @see #getCustomOption(ConfigOption)
     * @return options for this tx
     */
    Configuration getCustomOptions();
}
