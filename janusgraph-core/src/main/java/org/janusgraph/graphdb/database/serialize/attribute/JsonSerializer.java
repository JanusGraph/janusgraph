// Copyright 2018 JanusGraph Authors
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

import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.serialize.OrderPreservingSerializer;

import java.io.IOException;

/**
 * Serializes implementation of {@link org.apache.tinkerpop.shaded.jackson.databind.JsonNode} by using
 * {@link StringSerializer} for JSON string serialization.
 *
 * Throws {@link JsonNodeParseException} if the string has a wrong JSON format.
 *
 * @param <T> is the implementation of the JsonNode like
 *           {@link org.apache.tinkerpop.shaded.jackson.databind.node.ObjectNode} or
 *           {@link org.apache.tinkerpop.shaded.jackson.databind.node.ArrayNode}
 */
public class JsonSerializer<T extends JsonNode> implements OrderPreservingSerializer<T> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final StringSerializer stringSerializer = new StringSerializer();

    private final Class<T> jsonType;

    public JsonSerializer(Class<T> jsonType){
        this.jsonType = jsonType;
    }

    @Override
    public T readByteOrder(ScanBuffer buffer) {
        return parse(stringSerializer.readByteOrder(buffer));
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, T attribute) {
        stringSerializer.writeByteOrder(buffer, attribute.toString());
    }

    @Override
    public T read(ScanBuffer buffer) {
        return parse(stringSerializer.read(buffer));
    }

    @Override
    public void write(WriteBuffer buffer, T attribute) {
        stringSerializer.write(buffer, attribute.toString());
    }

    @Override
    public void verifyAttribute(T value) {
        stringSerializer.verifyAttribute(value.toString());
    }

    @Override
    public T convert(Object value) {
        if(jsonType.isAssignableFrom(value.getClass())){
            return jsonType.cast(value);
        }
        if(value instanceof String){
            return parse(stringSerializer.convert(value));
        }
        return null;
    }

    private T parse(String json){
        try {
            return jsonType.cast(OBJECT_MAPPER.readTree(json));
        } catch (IOException e) {
            throw new JsonNodeParseException(e);
        }
    }

}
