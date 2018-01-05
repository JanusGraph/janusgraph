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

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Optional;

/**
 * This enum unites different libraries' consistency level enums, streamlining
 * configuration and processing in {@link AbstractCassandraStoreManager}.
 *
 */
public enum CLevel implements CLevelInterface { // One ring to rule them all
    ANY,
    ONE,
    TWO,
    THREE,
    QUORUM,
    ALL,
    LOCAL_ONE,
    LOCAL_QUORUM,
    EACH_QUORUM;

    private final org.apache.cassandra.db.ConsistencyLevel db;
    private final org.apache.cassandra.thrift.ConsistencyLevel thrift;
    private final com.netflix.astyanax.model.ConsistencyLevel astyanax;

    CLevel() {
        db = org.apache.cassandra.db.ConsistencyLevel.valueOf(toString());
        thrift = org.apache.cassandra.thrift.ConsistencyLevel.valueOf(toString());
        astyanax = com.netflix.astyanax.model.ConsistencyLevel.valueOf("CL_" + toString());
    }

    @Override
    public org.apache.cassandra.db.ConsistencyLevel getDB() {
        return db;
    }

    @Override
    public org.apache.cassandra.thrift.ConsistencyLevel getThrift() {
        return thrift;
    }

    @Override
    public com.netflix.astyanax.model.ConsistencyLevel getAstyanax() {
        return astyanax;
    }

    public static CLevel parse(final String value) {
        Preconditions.checkArgument(value != null && !value.isEmpty());
        final String trimmed = value.trim();
        switch (trimmed) {
            case "1":
                return ONE;
            case "2":
                return TWO;
            case "3":
                return THREE;
            default:
                final Optional<CLevel> level = Arrays.stream(values())
                    .filter(c -> c.toString().equalsIgnoreCase(trimmed)
                            || ("CL_" + c.toString()).equalsIgnoreCase(trimmed))
                    .findFirst();
                if (level.isPresent()) {
                    return level.get();
                } else {
                    throw new IllegalArgumentException("Unrecognized cassandra consistency level: " + value);
                }
        }
    }
}
