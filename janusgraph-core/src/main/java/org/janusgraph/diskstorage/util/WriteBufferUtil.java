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
import org.janusgraph.diskstorage.WriteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class WriteBufferUtil {


    public static WriteBuffer put(WriteBuffer out, byte[] bytes) {
        for (byte aByte : bytes) out.putByte(aByte);
        return out;
    }

    public static WriteBuffer put(WriteBuffer out, StaticBuffer in) {
        for (int i=0;i<in.length();i++) out.putByte(in.getByte(i));
        return out;
    }

}
