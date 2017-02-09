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

package org.janusgraph.util.datastructures;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ByteSize {

    public static final int OBJECT_HEADER = 12;

    public static final int OBJECT_REFERENCE = 8;

    public static final int GUAVA_CACHE_ENTRY_SIZE = 104;

    public static final int GUAVA_CACHE_SOFT_ENTRY_SIZE = 136;


    //Does not include array contents of byte[]
    public static final int STATICARRAYBUFFER_RAW_SIZE = OBJECT_HEADER + 2*4 + 6 + (OBJECT_REFERENCE + OBJECT_HEADER + 8); // 6 = overhead & padding, (byte[] array)

    //Does not include wrapped array
    public static final int BYTEBUFFER_RAW_SIZE = OBJECT_HEADER + 4*4 + 8 + 4 + 1 + 4 + (OBJECT_REFERENCE + OBJECT_HEADER + 8); // 6 = overhead & padding, (byte[] array)


    public static final int ARRAYLIST_SIZE = OBJECT_HEADER + 4 + OBJECT_REFERENCE + OBJECT_HEADER + 6; // 4 = size, 6=padding


}
