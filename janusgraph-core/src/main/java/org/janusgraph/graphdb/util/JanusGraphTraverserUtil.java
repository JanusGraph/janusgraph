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

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.AbstractTraverser;
import org.reflections8.Reflections;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Reflection based helper tool to safely get `loops` for any Traverser implementation.
 */
public class JanusGraphTraverserUtil {

    private static final Set<Class<?>> TRAVERSERS_WITH_LOOP_SUPPORT = new HashSet<>();
    private static final Set<Class<?>> TRAVERSERS_WITHOUT_LOOP_SUPPORT = new HashSet<>();

    static {
        for(String packageToScan : Arrays.asList("org.apache.tinkerpop", "org.janusgraph")){
            Reflections reflections = new Reflections(packageToScan);
            Set<Class<? extends Traverser>> subTypesOfTraverser = reflections.getSubTypesOf(Traverser.class);
            for (Class<?> traverserType : subTypesOfTraverser) {
                addTraverserClass(traverserType);
            }
        }
    }

    private static void addTraverserClass(Class<?> traverserType){
        try {
            Class<?> declaredMethodClass = traverserType.getMethod("loops").getDeclaringClass();
            if(isLoopsPotentiallySupported(declaredMethodClass)){
                TRAVERSERS_WITH_LOOP_SUPPORT.add(traverserType);
            } else {
                TRAVERSERS_WITHOUT_LOOP_SUPPORT.add(traverserType);
            }
        } catch (NoSuchMethodException e) {
            TRAVERSERS_WITHOUT_LOOP_SUPPORT.add(traverserType);
        }
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
        if(TRAVERSERS_WITH_LOOP_SUPPORT.contains(traverser.getClass())){
            try{
                return traverser.loops();
            } catch (Exception e){
                // ignored
            }
        } else if(!TRAVERSERS_WITHOUT_LOOP_SUPPORT.contains(traverser.getClass())){
            // In case the Traverser implementation is not knows (i.e. it is an anonymous class or something which
            // isn't placed under standard TinkerPop or JanusGraph packages) then we add this implementation.
            addTraverserClass(traverser.getClass());
            return getLoops(traverser);
        }
        return 0;
    }

}
