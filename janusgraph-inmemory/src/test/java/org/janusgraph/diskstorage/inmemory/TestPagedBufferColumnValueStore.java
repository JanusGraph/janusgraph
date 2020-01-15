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

package org.janusgraph.diskstorage.inmemory;

/**
* This is a test version of SharedBufferColumnValueStore which overrides getMaxPageSize() to make page size configurable at runtime.
 * The "production" SharedBufferColumnValueStore doesn't allow this in order to reduce overhead of storing current page size
 * Changing page size to a different value is handy for modeling edge cases on smaller test data.
*/
class TestPagedBufferColumnValueStore extends InMemoryColumnValueStore
{
    private final int maxPageSize;

    public TestPagedBufferColumnValueStore(int maxPageSize)
    {
        super();
        this.maxPageSize = maxPageSize;
    }

    @Override
    public int getMaxPageSize()
    {
        return maxPageSize;
    }
}
