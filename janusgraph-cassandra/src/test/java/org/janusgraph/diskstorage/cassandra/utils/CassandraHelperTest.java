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

package org.janusgraph.diskstorage.cassandra.utils;

import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.junit.jupiter.api.Test;

import org.junit.Assert;

public class CassandraHelperTest {

    private static final BytesToken ZERO = new BytesToken(new byte[] {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});

    @Test
    public void testTransformRange() {
        BytesToken token2 = new BytesToken(new byte[] {-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,0,-1});
        KeyRange keyRange = CassandraHelper.transformRange(ZERO, token2);
        Assert.assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1}, keyRange.getStart().asByteBuffer().array());
        Assert.assertArrayEquals(new byte[] {-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,1,0}, keyRange.getEnd().asByteBuffer().array());
    }

    @Test
    public void testTransformRangeWithRollingCarry() {
        BytesToken token2 = new BytesToken(new byte[] {0,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1});
        KeyRange keyRange = CassandraHelper.transformRange(ZERO, token2);
        Assert.assertArrayEquals(new byte[] {1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, keyRange.getEnd().asByteBuffer().array());
    }
    
}
