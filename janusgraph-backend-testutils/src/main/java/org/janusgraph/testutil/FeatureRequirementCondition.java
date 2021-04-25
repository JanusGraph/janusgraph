// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.testutil;

import org.janusgraph.JanusGraphBaseStoreFeaturesTest;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContextException;
import org.junit.platform.commons.support.AnnotationSupport;
import org.opentest4j.TestAbortedException;

import java.util.Optional;

class FeatureRequirementCondition implements BeforeTestExecutionCallback {

    @Override
    public void beforeTestExecution(ExtensionContext context) throws Exception {
        FeatureFlag annotation = context.getTestMethod()
            .flatMap(testMethods -> AnnotationSupport.findAnnotation(testMethods, FeatureFlag.class))
            .orElseThrow(() -> new ExtensionContextException("The extension should not be executed "
                + "unless the test method is annotated with @FeatureRequirement."));

        Optional<Object> testInstance = context.getTestInstance();
        JanusGraphBaseStoreFeaturesTest test =  (JanusGraphBaseStoreFeaturesTest)testInstance.get();
        switch (annotation.feature()){
            case CellTtl:
                if (!test.getStoreFeatures().hasCellTTL()){
                    throw new TestAbortedException("Database doesn't support CellTtl.");
                }
                break;
            case Scan:
                if(!test.getStoreFeatures().hasScan()){
                    throw new TestAbortedException("Database doesn't support ordered/unordered scan.");
                }
                break;
            case OrderedScan:
                if(!test.getStoreFeatures().hasOrderedScan()){
                    throw new TestAbortedException("Database doesn't support ordered scan.");
                }
                break;
            case UnorderedScan:
                if(!test.getStoreFeatures().hasUnorderedScan()){
                    throw new TestAbortedException("Database doesn't support unordered scan.");
                }
                break;
            default:
                throw new UnsupportedOperationException("Feature Flag "+annotation.feature() + " is not supported.");
        }

    }
}
