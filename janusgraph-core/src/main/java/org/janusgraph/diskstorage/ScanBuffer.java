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
 * A Buffer that allows sequential reads.
 * Should not be used by multiple threads.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ScanBuffer {

    public boolean hasRemaining();

    public byte getByte();

    public boolean getBoolean();

    public short getShort();

    public int getInt();

    public long getLong();

    public char getChar();

    public float getFloat();

    public double getDouble();

    public byte[] getBytes(int length);

    public short[] getShorts(int length);

    public int[] getInts(int length);

    public long[] getLongs(int length);

    public char[] getChars(int length);

    public float[] getFloats(int length);

    public double[] getDoubles(int length);

}
