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

import org.janusgraph.diskstorage.util.time.TimestampProvider;


/**
 * Service provider interface for {@link LocalLockMediators}.
 */
public interface LocalLockMediatorProvider {

    /**
     * Returns a the single {@link LocalLockMediator} responsible for the
     * specified {@code namespace}.
     * <p>
     * For any given {@code namespace}, the same object must be returned every
     * time {@code get(n)} is called, no matter what thread calls it or how many
     * times.
     * <p>
     * For any two unequal namespace strings {@code n} and {@code m},
     * {@code get(n)} must not equal {@code get(m)}. in other words, each
     * namespace must have a distinct mediator.
     *
     * @param namespace
     *            the arbitrary identifier for a local lock mediator
     * @return the local lock mediator for {@code namespace}
     * @author Dan LaRocque (dalaro@hopcount.org)
     * @see LocalLockMediator
     */
    <T> LocalLockMediator<T> get(String namespace, TimestampProvider times);

}
