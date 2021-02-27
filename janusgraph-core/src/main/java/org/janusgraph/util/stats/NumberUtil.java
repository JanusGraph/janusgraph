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

package org.janusgraph.util.stats;

import com.google.common.base.Preconditions;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class NumberUtil {

    public static boolean isPowerOf2(long value) {
        return value>0 && Long.highestOneBit(value)==value;
    }

    /**
     * Returns an integer X such that 2^X=value. Throws an exception
     * if value is not a power of 2.
     *
     * @param value
     * @return
     */
    public static int getPowerOf2(long value) {
        Preconditions.checkArgument(isPowerOf2(value), "Value %d is not power of 2", value);
        return Long.SIZE-(Long.numberOfLeadingZeros(value)+1);
    }

}
