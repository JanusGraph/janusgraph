// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.io.binary.geoshape;

import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.janusgraph.core.attribute.Geoshape;

import java.io.IOException;

public abstract class GeoshapeTypeSerializer {

    private final int geoshapeTypeCode;

    protected GeoshapeTypeSerializer(final int geoshapeTypeCode) {
        this.geoshapeTypeCode = geoshapeTypeCode;
    }

    public abstract Geoshape readNonNullableGeoshapeValue(final Buffer buffer, final GraphBinaryReader context) throws IOException;

    public void writeNonNullableValue(final Geoshape geoshape, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        buffer.writeInt(geoshapeTypeCode);
        writeNonNullableGeoshapeValue(geoshape, buffer, context);
    }

    public abstract void writeNonNullableGeoshapeValue(final Geoshape geoshape, final Buffer buffer, final GraphBinaryWriter context) throws IOException;
}
