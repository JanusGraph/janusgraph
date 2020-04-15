// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.io.binary;

public enum GraphBinaryType {
    Geoshape(0x1000, "janusgraph.Geoshape"),
    RelationIdentifier(0x1001, "janusgraph.RelationIdentifier"),
    JanusGraphP(0x1002, "janusgraph.P");

    private int id;
    private String typeName;

    GraphBinaryType(int id, String typeName) {
        this.id = id;
        this.typeName = typeName;
    }

    public int getTypeId() {
        return id;
    }

    public String getTypeName() {
        return typeName;
    }
}
