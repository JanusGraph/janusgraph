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

package org.janusgraph.graphdb.tinkerpop.optimize.strategy;

import org.janusgraph.graphdb.configuration.ConfigName;

/**
 * Repeat step mode for registering batches for the multi-nested children step with batch support ({@link  org.janusgraph.graphdb.tinkerpop.optimize.step.MultiQueriable  MultiQueriable} steps).
 */
public enum MultiQueryStrategyRepeatStepMode implements ConfigName {

    /**
     * Child {@link  org.janusgraph.graphdb.tinkerpop.optimize.step.MultiQueriable  MultiQueriable} start steps will receive
     * vertices for batching from the closest outer `repeat` parent step only.<br>
     * For example:<br>
     * `g.V().repeat(and(repeat(out("knows")).emit())).emit()`
     * <br>
     * In the example above, `out("knows")` will be receiving vertices for batching from `and` step input because and
     * from the end of the `out(knows)` output.
     */
    CLOSEST_REPEAT_PARENT("closest_repeat_parent"),

    /**
     * Child {@link  org.janusgraph.graphdb.tinkerpop.optimize.step.MultiQueriable  MultiQueriable} start steps will receive
     * vertices for batching from the all outer `repeat` parent step only.
     * For example:<br>
     * `g.V().repeat(and(repeat(out("knows")).emit())).emit()`
     * <br>
     * In the example above, `out("knows")` will be receiving vertices for batching from the most outer `repeat` step input,
     * the most outer `repeat` step output (which is `and` output), the `and` step input, and from the `out("knows")` output.
     */
    ALL_REPEAT_PARENTS("all_repeat_parents"),

    /**
     * Child {@link  org.janusgraph.graphdb.tinkerpop.optimize.step.MultiQueriable  MultiQueriable} start steps will receive
     * vertices for batching from the closest outer `repeat` parent step from the step start and next iteration step results as
     * well as from all starts of all outer `repeat` parent steps.
     * For example:<br>
     * `g.V().repeat(and(repeat(out("knows")).emit())).emit()`
     * <br>
     * In the example above, `out("knows")` will be receiving vertices for batching from the most outer `repeat` step input,
     * the `and` step input, and from the `out("knows")` output.
     */
    STARTS_ONLY_OF_ALL_REPEAT_PARENTS("starts_only_of_all_repeat_parents")
    ;

    private final String configurationOptionName;

    MultiQueryStrategyRepeatStepMode(String configurationOptionName){
        this.configurationOptionName = configurationOptionName;
    }

    @Override
    public String getConfigName() {
        return configurationOptionName;
    }
}
