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

import org.janusgraph.core.JanusGraphTransaction;

import lombok.extern.slf4j.Slf4j;

/**
 * This class may become obsolete in the future, at which point it will 
 * be deprecated or removed. This follows from the assumption that 
 * try-with-resources and transactions implement AutoCloseable.
 */
@Slf4j
public class TXUtils {

    public static void rollbackQuietly(JanusGraphTransaction tx) {
        if (null == tx)
            return;

        try {
            tx.rollback();
        } catch (Throwable t) {
            log.warn("Unable to rollback transaction", t);
        }
    }
}
