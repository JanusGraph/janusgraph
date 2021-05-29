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

package org.janusgraph.hadoop.serialize;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.spark.serializer.KryoRegistrator;
import org.janusgraph.core.attribute.Geoshape;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Register JanusGraph classes requiring custom Kryo serialization for Spark.
 *
 */
public class JanusGraphKryoRegistrator implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(Geoshape.class, new GeoShapeKryoSerializer());
    }

    /**
     * Geoshape serializer for Kryo.
     */
    public static class GeoShapeKryoSerializer extends Serializer<Geoshape> {
        @Override
        public void write(Kryo kryo, Output output, Geoshape geoshape) {
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                Geoshape.GeoshapeBinarySerializer.write(outputStream, geoshape);
                byte[] bytes = outputStream.toByteArray();
                output.write(bytes.length);
                output.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException("I/O exception writing geoshape");
            }
        }

        @Override
        public Geoshape read(Kryo kryo, Input input, Class<Geoshape> aClass) {
            int length = input.read();
            Preconditions.checkArgument(length>0);
            try (InputStream inputStream = new ByteArrayInputStream(input.readBytes(length))) {
                return Geoshape.GeoshapeBinarySerializer.read(inputStream);
            } catch (IOException e) {
                throw new RuntimeException("I/O exception reading geoshape");
            }
        }
    }
}
