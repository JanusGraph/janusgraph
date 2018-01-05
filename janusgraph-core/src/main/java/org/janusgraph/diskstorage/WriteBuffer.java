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

package org.janusgraph.diskstorage;

/**
 * A Buffer that allows simple writes and returns the result as a {@link StaticBuffer}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface WriteBuffer {


    WriteBuffer putByte(byte val);

    WriteBuffer putBytes(byte[] val);

    WriteBuffer putBytes(StaticBuffer val);

    WriteBuffer putBoolean(boolean val);

    WriteBuffer putShort(short val);

    WriteBuffer putInt(int val);

    WriteBuffer putLong(long val);

    WriteBuffer putChar(char val);

    WriteBuffer putFloat(float val);

    WriteBuffer putDouble(double val);

    StaticBuffer getStaticBuffer();

    int getPosition();

    StaticBuffer getStaticBufferFlipBytes(int from, int to);

}
