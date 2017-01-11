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


    public WriteBuffer putByte(byte val);

    public WriteBuffer putBytes(byte[] val);

    public WriteBuffer putBytes(StaticBuffer val);

    public WriteBuffer putBoolean(boolean val);

    public WriteBuffer putShort(short val);

    public WriteBuffer putInt(int val);

    public WriteBuffer putLong(long val);

    public WriteBuffer putChar(char val);

    public WriteBuffer putFloat(float val);

    public WriteBuffer putDouble(double val);

    public StaticBuffer getStaticBuffer();

    public int getPosition();

    public StaticBuffer getStaticBufferFlipBytes(int from, int to);

}
