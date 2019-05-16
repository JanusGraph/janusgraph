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

public class RetryInfo {
    private final int invocation;
    private final int maxInvocations;

    public RetryInfo(int invocation, int maxInvocations) {
        this.invocation = invocation;
        this.maxInvocations = maxInvocations;
    }

    public int getInvocation() {
        return invocation;
    }

    @SuppressWarnings("unused")
    public int getMaxInvocations() {
        return maxInvocations;
    }
}
