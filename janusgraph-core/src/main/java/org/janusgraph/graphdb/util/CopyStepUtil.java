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

import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;

public class CopyStepUtil {

    private CopyStepUtil() {}

    public static void copyAbstractStepModifiableFields(AbstractStep<?,?> originalStep, AbstractStep<?,?> copiedStep){
        for(String label : originalStep.getLabels()){
            copiedStep.addLabel(label);
        }
    }

}
