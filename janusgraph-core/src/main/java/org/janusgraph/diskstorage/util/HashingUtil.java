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

import com.google.common.hash.HashCode;
import org.janusgraph.diskstorage.StaticBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class HashingUtil {

    public enum HashLength {
        SHORT, LONG;

        public int length() {
            switch (this) {
                case SHORT: return 4;
                case LONG: return 8;
                default: throw new AssertionError("Unknown hash type: " + this);
            }
        }
    }

    private static final StaticBuffer.Factory<HashCode> SHORT_HASH_FACTORY = (array, offset, limit) -> HashUtility.SHORT.get().hashBytes(array, offset, limit);

    private static final StaticBuffer.Factory<HashCode> LONG_HASH_FACTORY = (array, offset, limit) -> HashUtility.LONG.get().hashBytes(array,offset,limit);

    public static StaticBuffer hashPrefixKey(final HashLength hashPrefixLen, final StaticBuffer key) {
        final int prefixLen = hashPrefixLen.length();
        final StaticBuffer.Factory<HashCode> hashFactory;
        switch (hashPrefixLen) {
            case SHORT:
                hashFactory = SHORT_HASH_FACTORY;
                break;
            case LONG:
                hashFactory = LONG_HASH_FACTORY;
                break;
            default: throw new IllegalArgumentException("Unknown hash prefix: " + hashPrefixLen);
        }

        HashCode hashcode = key.as(hashFactory);
        WriteByteBuffer newKey = new WriteByteBuffer(prefixLen+key.length());
        assert prefixLen==4 || prefixLen==8;
        if (prefixLen==4) newKey.putInt(hashcode.asInt());
        else newKey.putLong(hashcode.asLong());
        newKey.putBytes(key);
        return newKey.getStaticBuffer();
    }

    public static StaticBuffer getKey(final HashLength hashPrefixLen, StaticBuffer hasPrefixedKey) {
        return hasPrefixedKey.subrange(hashPrefixLen.length(), hasPrefixedKey.length() - hashPrefixLen.length());
    }



}
