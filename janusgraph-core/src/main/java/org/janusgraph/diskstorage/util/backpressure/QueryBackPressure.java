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

package org.janusgraph.diskstorage.util.backpressure;

public interface QueryBackPressure {

    /**
     * Acquires one slot. If not slots are available then this method will be blocked until a slot is available.
     */
    void acquireBeforeQuery();

    /**
     * Releases one slot in non-blocking fashion. The successful execution of this method doesn't mean that
     * the release is happened. Instead, execution of this method guarantees that the release will happen.
     */
    void releaseAfterQuery();

    /**
     * Releases any resources related to the back pressure mechanism. Makes this instance unusable after that.
     */
    void close();

}
