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

// Based on Stackoverflow post: https://stackoverflow.com/a/46207476/2978513
// License: https://choosealicense.com/licenses/unlicense/

package org.janusgraph.testutil;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContextException;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.util.AnnotationUtils;

import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FlakyTestExtension implements TestTemplateInvocationContextProvider {

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return AnnotationUtils.isAnnotated(context.getTestMethod(), FlakyTest.class);
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        FlakyTest annotation = context.getTestMethod()
            .flatMap(testMethods -> AnnotationSupport.findAnnotation(testMethods, FlakyTest.class))
            .orElseThrow(() -> new ExtensionContextException("The extension should not be executed "
                + "unless the test method is annotated with @FlakyTest."));

        checkValidRetry(annotation);

        return IntStream.rangeClosed(1, annotation.invocationCount())
            .mapToObj((it) -> new RetryTemplateContext(it, annotation.invocationCount(), annotation.minSuccess()));
    }

    private void checkValidRetry(FlakyTest annotation) {
        if (annotation.invocationCount() < 1) {
            throw new ExtensionContextException(annotation.invocationCount() + " must be greater than or equal to 1");
        }
        if (annotation.minSuccess() < 1 || annotation.minSuccess() > annotation.invocationCount()) {
            throw new ExtensionContextException("Invalid "+ annotation.minSuccess());
        }
    }
}
