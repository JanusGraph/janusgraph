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

package org.janusgraph.diskstorage.cassandra;

import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.janusgraph.CassandraTestCategory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag(CassandraTestCategory.STANDALONE_TESTS)
public class UUIDTest {
    public static final String z = "00000000-0000-1000-0000-000000000000";
    public static final String v = "9451e273-7753-11e0-92df-e700f669bcfc";

    @Test
    public void timeUUIDComparison() {
        TimeUUIDType ti = TimeUUIDType.instance;

        UUID zu = UUID.fromString(z);
        UUID vu = UUID.fromString(v);

        ByteBuffer zb = ti.decompose(zu);
        ByteBuffer vb = ti.decompose(vu);

        assertEquals(-1, ti.compare(zb, vb));
        assertEquals(1, zu.compareTo(vu));
        assertEquals(1, ti.compare(vb, zb));
        assertEquals(-1, vu.compareTo(zu));
    }
}
