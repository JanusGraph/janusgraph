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

package org.janusgraph.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ObjectSerializer implements AttributeSerializer<Object> {

    @Override
    public Object read(ScanBuffer buffer) {
        Preconditions.checkArgument(buffer.getByte()==1,"Invalid serialization state");
        return new Object();
    }

    @Override
    public void write(WriteBuffer buffer, Object attribute) {
        buffer.putByte((byte)1);
    }

}
