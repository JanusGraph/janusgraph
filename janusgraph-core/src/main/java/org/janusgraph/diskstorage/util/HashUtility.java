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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum HashUtility {

    SHORT {
        @Override
        public HashFunction get() {
            return Hashing.murmur3_32();
        }
    },

    LONG {
        @Override
        public HashFunction get() {
            return Hashing.murmur3_128();
        }
    };


    public abstract HashFunction get();


}
