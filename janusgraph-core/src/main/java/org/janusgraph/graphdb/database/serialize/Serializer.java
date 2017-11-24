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

package org.janusgraph.graphdb.database.serialize;

import org.janusgraph.diskstorage.ScanBuffer;

import java.io.Closeable;

public interface Serializer extends AttributeHandler, Closeable {

    Object readClassAndObject(ScanBuffer buffer);

    <T> T readObject(ScanBuffer buffer, Class<T> type);

    <T> T readObjectByteOrder(ScanBuffer buffer, Class<T> type);

    <T> T readObjectNotNull(ScanBuffer buffer, Class<T> type);

    DataOutput getDataOutput(int initialCapacity);

}
