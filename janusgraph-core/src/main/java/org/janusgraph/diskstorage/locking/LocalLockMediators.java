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

package org.janusgraph.diskstorage.locking;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A singleton maintaining a globally unique map of {@link LocalLockMediator}
 * instances.
 *
 * @see LocalLockMediatorProvider
 * @author Dan LaRocque (dalaro@hopcount.org)
 */
public enum LocalLockMediators implements LocalLockMediatorProvider {
    INSTANCE;

    private static final Logger log = LoggerFactory
            .getLogger(LocalLockMediators.class);

    /**
     * Maps a namespace to the mediator responsible for the namespace.
     * <p>
     * Implementation note: for Cassandra, namespace is usually a column
     * family name.
     */
    private final ConcurrentHashMap<String, LocalLockMediator<?>> mediators = new ConcurrentHashMap<>();

    @Override
    public <T> LocalLockMediator<T> get(String namespace, TimestampProvider times) {

        Preconditions.checkNotNull(namespace);

        @SuppressWarnings("unchecked")
        LocalLockMediator<T> m = (LocalLockMediator<T>)mediators.get(namespace);

        if (null == m) {
            m = new LocalLockMediator<>(namespace, times);
            @SuppressWarnings("unchecked")
            final LocalLockMediator<T> old = (LocalLockMediator<T>)mediators.putIfAbsent(namespace, m);
            if (null != old)
                m = old;
            else
                log.debug("Local lock mediator instantiated for namespace {}",
                        namespace);
        }

        return m;
    }

    /**
     * Only use this in testing.
     * <p>
     * This deletes the global map of namespaces to mediators. Calling this in
     * production would result in undetected locking failures and data
     * corruption.
     */
    public void clear() {
        mediators.clear();
    }

    /**
     * Only use this in testing.
     * <p>
     * This deletes all entries in the global map of namespaces to mediators
     * whose namespace key equals the argument.
     *
     * @param namespace
     */
    public void clear(String namespace) {
        mediators.entrySet().removeIf(e -> e.getKey().equals(namespace));
    }
}
