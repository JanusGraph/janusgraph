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
package org.janusgraph.graphdb.tinkerpop.optimize.step;

public class Aggregation {
    public enum Type {
        COUNT, MIN, MAX, AVG, SUM
    }

    private final Type type;
    private String fieldName;
    private Class dataType;

    private Aggregation(Type type, String fieldName) {
        this.type = type;
        this.fieldName = fieldName;
        this.dataType = null;
    }

    public Type getType() {
        return type;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Class getDataType() {
        return dataType;
    }

    public void setDataType(Class dataType) {
        this.dataType = dataType;
    }

    public static final Aggregation COUNT = new Aggregation(Type.COUNT, null);

    public static Aggregation MIN(String fieldName) {
        return new Aggregation(Type.MIN, fieldName);
    }

    public static Aggregation MAX(String fieldName) {
        return new Aggregation(Type.MAX, fieldName);
    }

    public static Aggregation AVG(String fieldName) {
        return new Aggregation(Type.AVG, fieldName);
    }

    public static Aggregation SUM(String fieldName) {
        return new Aggregation(Type.SUM, fieldName);
    }

}

