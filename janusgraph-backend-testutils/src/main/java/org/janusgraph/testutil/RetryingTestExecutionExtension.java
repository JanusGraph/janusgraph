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

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.opentest4j.TestAbortedException;

import java.util.ArrayList;
import java.util.List;

public class RetryingTestExecutionExtension implements ExecutionCondition, ParameterResolver, TestExecutionExceptionHandler {
    private final int invocation;
    private final int maxInvocations;
    private final int minSuccess;

    public RetryingTestExecutionExtension(int invocation, int maxInvocations, int minSuccess) {
        this.invocation = invocation;
        this.maxInvocations = maxInvocations;
        this.minSuccess = minSuccess;
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        int failureCount = getFailures(context).size();
        // Shift -1 because this happens before test
        int successCount = (invocation - 1) - failureCount;
        if ((maxInvocations - failureCount) < minSuccess) {
            return ConditionEvaluationResult.disabled("Cannot hit minimum success rate of " + minSuccess + "/" + maxInvocations + " - " + failureCount + " failures already");
        } else if (successCount < minSuccess) {
            return ConditionEvaluationResult.enabled("Have not ran " + minSuccess + "/" + maxInvocations + " successful executions");
        }
        return ConditionEvaluationResult.disabled(minSuccess + "/" + maxInvocations + " successful runs have already ran. Skipping " + invocation);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType() == RetryInfo.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return new RetryInfo(invocation, maxInvocations);
    }

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        RetryingTestFailure testFailure = new RetryingTestFailure(invocation, throwable);
        List<RetryingTestFailure> failures = getFailures(context);
        failures.add(testFailure);
        int failureCount = failures.size();
        int successCount = invocation - failureCount;
        if ((maxInvocations - failureCount) < minSuccess) {
            throw testFailure;
        } else if (successCount < minSuccess) {
            // Case when we have still have retries left
            throw new TestAbortedException("Aborting test #" + minSuccess + " of " + maxInvocations + "- still have retries left", testFailure);
        }
    }

    private List<RetryingTestFailure> getFailures(ExtensionContext context) {
        ExtensionContext.Namespace namespace = ExtensionContext.Namespace.create(RetryingTestExecutionExtension.class);
        ExtensionContext.Store store = context.getParent().get().getStore(namespace);

        //noinspection unchecked
        return (List<RetryingTestFailure>) store.getOrComputeIfAbsent(context.getRequiredTestMethod().getName(), (a) -> new ArrayList<RetryingTestFailure>(), List.class);
    }
}
