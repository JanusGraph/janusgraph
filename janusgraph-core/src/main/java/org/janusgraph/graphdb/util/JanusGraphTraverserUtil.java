// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.graphdb.util;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.AbstractTraverser;

/**
 * Reflection based helper tool to safely get `loops` for any Traverser implementation.
 */
public class JanusGraphTraverserUtil {

    // Whether a Traverser implementation may support `loops`, worked out once per class when a traverser of it is first
    // seen. A ClassValue is safe to read and to fill from any thread, and doesn't keep the class from being unloaded
    private static final ClassValue<Boolean> LOOPS_POTENTIALLY_SUPPORTED = new ClassValue<Boolean>() {
        @Override
        protected Boolean computeValue(Class<?> traverserType) {
            try {
                return isLoopsPotentiallySupported(traverserType.getMethod("loops").getDeclaringClass());
            } catch (NoSuchMethodException e) {
                return false;
            } catch (SecurityException e) {
                // Where reflection is denied, the call itself may still be allowed: getLoops() tries it, and falls
                // back to 0 when it fails
                return true;
            }
        }
    };

    @VisibleForTesting
    static boolean isLoopsPotentiallySupportedBy(Class<? extends Traverser> traverserType) {
        return LOOPS_POTENTIALLY_SUPPORTED.get(traverserType);
    }

    private static boolean isLoopsPotentiallySupported(Class<?> type){
        return !type.equals(AbstractTraverser.class) && !type.equals(Traverser.class);
    }

    /**
     * Get loop number for Traverser which supports it. In case Traverser doesn't support `loops` then `0` will be
     * returned.
     * @param traverser Any traverser implementation
     * @return `loops` result if Traverser implementation supports it or `0` otherwise.
     */
    public static int getLoops(Traverser<?> traverser){
        if (LOOPS_POTENTIALLY_SUPPORTED.get(traverser.getClass())) {
            try{
                return traverser.loops();
            } catch (Exception e){
                // ignored
            }
        }
        return 0;
    }

}
