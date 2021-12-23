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

package org.janusgraph.graphdb.grpc.schema.util;

import com.google.protobuf.Any;
import com.google.protobuf.Int64Value;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphSchemaType;
import org.janusgraph.graphdb.grpc.types.EdgeLabel;
import org.janusgraph.graphdb.grpc.types.EdgeProperty;
import org.janusgraph.graphdb.grpc.types.PropertyDataType;
import org.janusgraph.graphdb.grpc.types.VertexCompositeGraphIndex;
import org.janusgraph.graphdb.grpc.types.VertexLabel;
import org.janusgraph.graphdb.grpc.types.VertexProperty;

import java.util.Collection;
import java.util.Date;
import java.util.UUID;

public class GrpcUtils {

    public static Any getIdProtoForElement(JanusGraphSchemaType element) {
        return Any.pack(Int64Value.of(element.longId()));
    }

    public static Class<?> convertGrpcPropertyDataType(PropertyDataType propertyDataType) {
        switch (propertyDataType) {
            case PROPERTY_DATA_TYPE_CHARACTER:
                return Character.class;
            case PROPERTY_DATA_TYPE_BOOLEAN:
                return Boolean.class;
            case PROPERTY_DATA_TYPE_INT8:
                return Byte.class;
            case PROPERTY_DATA_TYPE_INT16:
                return Short.class;
            case PROPERTY_DATA_TYPE_INT32:
                return Integer.class;
            case PROPERTY_DATA_TYPE_INT64:
                return Long.class;
            case PROPERTY_DATA_TYPE_FLOAT32:
                return Float.class;
            case PROPERTY_DATA_TYPE_FLOAT64:
                return Double.class;
            case PROPERTY_DATA_TYPE_DATE:
                return Date.class;
            case PROPERTY_DATA_TYPE_GEO_SHAPE:
                return Geoshape.class;
            case PROPERTY_DATA_TYPE_STRING:
                return String.class;
            case PROPERTY_DATA_TYPE_UUID:
                return UUID.class;
            case PROPERTY_DATA_TYPE_JAVA_OBJECT:
            default:
                return Object.class;
        }
    }

    public static PropertyDataType convertToGrpcPropertyDataType(Class<?> propertyDataType) {
        if (propertyDataType == Boolean.class) {
            return PropertyDataType.PROPERTY_DATA_TYPE_BOOLEAN;
        }
        if (propertyDataType == Character.class) {
            return PropertyDataType.PROPERTY_DATA_TYPE_CHARACTER;
        }
        if (propertyDataType == Byte.class) {
            return PropertyDataType.PROPERTY_DATA_TYPE_INT8;
        }
        if (propertyDataType == Short.class) {
            return PropertyDataType.PROPERTY_DATA_TYPE_INT16;
        }
        if (propertyDataType == Integer.class) {
            return PropertyDataType.PROPERTY_DATA_TYPE_INT32;
        }
        if (propertyDataType == Long.class) {
            return PropertyDataType.PROPERTY_DATA_TYPE_INT64;
        }
        if (propertyDataType == Float.class) {
            return PropertyDataType.PROPERTY_DATA_TYPE_FLOAT32;
        }
        if (propertyDataType == Double.class) {
            return PropertyDataType.PROPERTY_DATA_TYPE_FLOAT64;
        }
        if (propertyDataType == Date.class) {
            return PropertyDataType.PROPERTY_DATA_TYPE_DATE;
        }
        if (propertyDataType == Geoshape.class) {
            return PropertyDataType.PROPERTY_DATA_TYPE_GEO_SHAPE;
        }
        if (propertyDataType == String.class) {
            return PropertyDataType.PROPERTY_DATA_TYPE_STRING;
        }
        if (propertyDataType == UUID.class) {
            return PropertyDataType.PROPERTY_DATA_TYPE_UUID;
        }
        return PropertyDataType.PROPERTY_DATA_TYPE_JAVA_OBJECT;
    }

    public static Cardinality convertGrpcCardinality(VertexProperty.Cardinality cardinality) {
        switch (cardinality) {
            case CARDINALITY_LIST:
                return Cardinality.LIST;
            case CARDINALITY_SET:
                return Cardinality.SET;
            case CARDINALITY_SINGLE:
            default:
                return Cardinality.SINGLE;
        }
    }

    public static VertexProperty.Cardinality convertToGrpcCardinality(Cardinality cardinality) {
        switch (cardinality) {
            case LIST:
                return VertexProperty.Cardinality.CARDINALITY_LIST;
            case SET:
                return VertexProperty.Cardinality.CARDINALITY_SET;
            case SINGLE:
            default:
                return VertexProperty.Cardinality.CARDINALITY_SINGLE;
        }
    }

    public static VertexLabel createVertexLabelProto(org.janusgraph.core.VertexLabel vertexLabel) {
        VertexLabel.Builder builder = VertexLabel.newBuilder()
            .setId(getIdProtoForElement(vertexLabel))
            .setName(vertexLabel.name())
            .setPartitioned(vertexLabel.isPartitioned())
            .setReadOnly(vertexLabel.isStatic());
        Collection<PropertyKey> propertyKeys = vertexLabel.mappedProperties();
        for (PropertyKey propertyKey : propertyKeys) {
            builder.addProperties(createVertexPropertyProto(propertyKey));
        }
        return builder.build();
    }

    private static VertexProperty createVertexPropertyProto(PropertyKey propertyKey) {
        return VertexProperty.newBuilder()
            .setId(getIdProtoForElement(propertyKey))
            .setDataType(convertToGrpcPropertyDataType(propertyKey.dataType()))
            .setCardinality(convertToGrpcCardinality(propertyKey.cardinality()))
            .setName(propertyKey.name())
            .build();
    }

    public static Multiplicity convertGrpcEdgeMultiplicity(EdgeLabel.Multiplicity multiplicity) {
        switch (multiplicity) {
            case MULTIPLICITY_MANY2ONE:
                return Multiplicity.MANY2ONE;
            case MULTIPLICITY_ONE2MANY:
                return Multiplicity.ONE2MANY;
            case MULTIPLICITY_ONE2ONE:
                return Multiplicity.ONE2ONE;
            case MULTIPLICITY_SIMPLE:
                return Multiplicity.SIMPLE;
            case MULTIPLICITY_MULTI:
            default:
                return Multiplicity.MULTI;
        }
    }

    public static EdgeLabel.Multiplicity convertToGrpcMultiplicity(Multiplicity multiplicity) {
        switch (multiplicity) {
            case SIMPLE:
                return EdgeLabel.Multiplicity.MULTIPLICITY_SIMPLE;
            case ONE2MANY:
                return EdgeLabel.Multiplicity.MULTIPLICITY_ONE2MANY;
            case MANY2ONE:
                return EdgeLabel.Multiplicity.MULTIPLICITY_MANY2ONE;
            case ONE2ONE:
                return EdgeLabel.Multiplicity.MULTIPLICITY_ONE2ONE;
            case MULTI:
            default:
                return EdgeLabel.Multiplicity.MULTIPLICITY_MULTI;
        }
    }

    public static EdgeLabel createEdgeLabelProto(org.janusgraph.core.EdgeLabel edgeLabel) {
        EdgeLabel.Builder builder = EdgeLabel.newBuilder()
            .setId(getIdProtoForElement(edgeLabel))
            .setName(edgeLabel.name())
            .setMultiplicity(convertToGrpcMultiplicity(edgeLabel.multiplicity()))
            .setDirection(edgeLabel.isDirected() ? EdgeLabel.Direction.DIRECTION_BOTH : EdgeLabel.Direction.DIRECTION_OUT);
        Collection<PropertyKey> propertyKeys = edgeLabel.mappedProperties();
        for (PropertyKey propertyKey : propertyKeys) {
            builder.addProperties(createEdgePropertyProto(propertyKey));
        }
        return builder.build();
    }

    private static EdgeProperty createEdgePropertyProto(PropertyKey propertyKey) {
        return EdgeProperty.newBuilder()
            .setId(getIdProtoForElement(propertyKey))
            .setDataType(convertToGrpcPropertyDataType(propertyKey.dataType()))
            .setName(propertyKey.name())
            .build();
    }

    public static Any getIdProtoForElement(JanusGraphIndex index) {
        return Any.pack(Int64Value.of(index.longId()));
    }

    public static VertexCompositeGraphIndex createVertexCompositeGraphIndex(JanusGraphIndex graphIndex) {
        VertexCompositeGraphIndex.Builder builder = VertexCompositeGraphIndex.newBuilder()
            .setId(getIdProtoForElement(graphIndex))
            .setName(graphIndex.name())
            .setUnique(graphIndex.isUnique());
        for (PropertyKey fieldKey : graphIndex.getFieldKeys()) {
            builder.addKeys(GrpcUtils.createVertexPropertyProto(fieldKey));
        }
        JanusGraphSchemaType schemaTypeConstraint = graphIndex.getSchemaTypeConstraint();
        if (schemaTypeConstraint instanceof org.janusgraph.core.VertexLabel) {
            builder.setIndexOnly(GrpcUtils.createVertexLabelProto((org.janusgraph.core.VertexLabel)schemaTypeConstraint));
        }
        return builder.build();
    }
}
