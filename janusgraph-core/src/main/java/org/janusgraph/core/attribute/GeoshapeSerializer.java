// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.core.attribute;

import com.google.common.base.Preconditions;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.diskstorage.util.ReadArrayBuffer;
import org.janusgraph.graphdb.database.idhandling.VariableLong;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Geoshape attribute serializer for JanusGraph.
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class GeoshapeSerializer implements AttributeSerializer<Geoshape> {

    @Override
    public void verifyAttribute(Geoshape value) {
        //All values of Geoshape are valid
    }

    @Override
    public Geoshape convert(Object value) {
        if(value instanceof Map) {
            return convertGeoJson(value);
        }

        if(value instanceof Collection) {
            value = convertCollection((Collection<Object>) value);
        }

        if (value.getClass().isArray() && (value.getClass().getComponentType().isPrimitive() ||
            Number.class.isAssignableFrom(value.getClass().getComponentType())) ) {
            int len= Array.getLength(value);
            double[] arr = new double[len];
            for (int i=0;i<len;i++) arr[i]=((Number)Array.get(value,i)).doubleValue();
            switch (len) {
                case 2:
                    return Geoshape.point(arr[0], arr[1]);
                case 3:
                    return Geoshape.circle(arr[0], arr[1], arr[2]);
                case 4:
                    return Geoshape.box(arr[0], arr[1], arr[2], arr[3]);
                default:
                    throw new IllegalArgumentException("Expected 2-4 coordinates to create Geoshape, but given: " + value);
            }
        } else if (value instanceof String) {
            String[] components=null;
            for (String delimiter : new String[]{",",";"}) {
                components = ((String)value).split(delimiter);
                if (components.length>=2 && components.length<=4) break;
                else components=null;
            }
            Preconditions.checkNotNull(components, "Could not parse coordinates from string: %s",value);
            double[] coordinates = new double[components.length];
            try {
                for (int i=0;i<components.length;i++) {
                    coordinates[i]=Double.parseDouble(components[i]);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Could not parse coordinates from string: " + value, e);
            }
            return convert(coordinates);
        } else return null;
    }

    private double[] convertCollection(Collection<Object> c) {
        Object[] boxedElements = c.toArray(new Object[0]);
        double[] unboxedElements = new double[boxedElements.length];
        for(int i=0; i<boxedElements.length;i++){
            Object element = boxedElements[i];
            if (!(element instanceof Number)) {
                throw new IllegalArgumentException("Collections may only contain numbers to create a Geoshape");
            }
            unboxedElements[i] = ((Number) element).doubleValue();
        }
        return unboxedElements;
    }

    private Geoshape convertGeoJson(Object value) {
        //Note that geoJson is long,lat
        try {
            Map<String, Object> map = (Map) value;
            String type = (String) map.get("type");
            if("Feature".equals(type)) {
                Map<String, Object> geometry = (Map) map.get("geometry");
                return convertGeometry(geometry);
            } else {
                return convertGeometry(map);
            }
        } catch (ClassCastException | IOException | ParseException e) {
            throw new IllegalArgumentException("GeoJSON was unparsable");
        }
    }

    private Geoshape convertGeometry(Map<String, Object> geometry) throws IOException, ParseException {
        String type = (String) geometry.get("type");
        List<Object> coordinates = (List) geometry.get(Geoshape.FIELD_COORDINATES);

        switch (type) {
            case "Point": {
                double[] parsedCoordinates = convertCollection(coordinates);
                return Geoshape.point(parsedCoordinates[1], parsedCoordinates[0]);
            }
            case "Circle": {
                Number radius = (Number) geometry.get("radius");
                if (radius == null) {
                    throw new IllegalArgumentException("GeoJSON circles require a radius");
                }
                double[] parsedCoordinates = convertCollection(coordinates);
                return Geoshape.circle(parsedCoordinates[1], parsedCoordinates[0], radius.doubleValue());
            }
            case "Polygon":
                // check whether this is a box
                if (coordinates.size() == 4) {
                    double[] p0 = convertCollection((Collection) coordinates.get(0));
                    double[] p1 = convertCollection((Collection) coordinates.get(1));
                    double[] p2 = convertCollection((Collection) coordinates.get(2));
                    double[] p3 = convertCollection((Collection) coordinates.get(3));

                    //This may be a clockwise or counterclockwise polygon, we have to verify that it is a box
                    if ((p0[0] == p1[0] && p1[1] == p2[1] && p2[0] == p3[0] && p3[1] == p0[1] && p3[0] != p0[0]) ||
                        (p0[1] == p1[1] && p1[0] == p2[0] && p2[1] == p3[1] && p3[0] == p0[0] && p3[1] != p0[1])) {
                        return Geoshape.box(min(p0[1], p1[1], p2[1], p3[1]), min(p0[0], p1[0], p2[0], p3[0]), max(p0[1], p1[1], p2[1], p3[1]), max(p0[0], p1[0], p2[0], p3[0]));
                    }
                }
                break;
        }

        String json = Geoshape.mapWriter.writeValueAsString(geometry);
        return new Geoshape(Geoshape.HELPER.getGeojsonReader().read(new StringReader(json)));
    }

    private double min(double... numbers) {
        return Arrays.stream(numbers).min().getAsDouble();
    }

    private double max(double... numbers) {
        return Arrays.stream(numbers).max().getAsDouble();
    }


    @Override
    public Geoshape read(ScanBuffer buffer) {
        long l = VariableLong.readPositive(buffer);
        assert l>0 && l<Integer.MAX_VALUE;
        int length = (int)l;
        int position = ((ReadArrayBuffer) buffer).getPosition();
        InputStream inputStream = new ByteArrayInputStream(buffer.getBytes(length));
        try {
            return Geoshape.GeoshapeBinarySerializer.read(inputStream);
        } catch (IOException e) {
            // retry using legacy point deserialization
            try {
                ((ReadArrayBuffer) buffer).movePositionTo(position);
                final float lat = buffer.getFloat();
                final float lon = buffer.getFloat();
                return Geoshape.point(lat, lon);
            } catch (Exception ignored) { }
            // throw original exception
            throw new RuntimeException("I/O exception reading geoshape", e);
        }
    }

    @Override
    public void write(WriteBuffer buffer, Geoshape attribute) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Geoshape.GeoshapeBinarySerializer.write(outputStream, attribute);
            byte[] bytes = outputStream.toByteArray();
            VariableLong.writePositive(buffer,bytes.length);
            buffer.putBytes(bytes);
        } catch (IOException e) {
            throw new RuntimeException("I/O exception writing geoshape", e);
        }
    }
}

