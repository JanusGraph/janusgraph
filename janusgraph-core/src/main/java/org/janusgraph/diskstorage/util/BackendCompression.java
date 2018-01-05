// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.diskstorage.util;

import org.janusgraph.diskstorage.StaticBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface BackendCompression {

    StaticBuffer compress(StaticBuffer value);

    StaticBuffer decompress(StaticBuffer value);

    BackendCompression NO_COMPRESSION = new BackendCompression() {
        @Override
        public StaticBuffer compress(StaticBuffer value) {
            return value;
        }

        @Override
        public StaticBuffer decompress(StaticBuffer value) {
            return value;
        }
    };

}
