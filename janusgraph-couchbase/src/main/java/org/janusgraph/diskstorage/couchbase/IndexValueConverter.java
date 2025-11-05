/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.janusgraph.diskstorage.couchbase;

import org.janusgraph.core.attribute.Geoshape;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;

import java.util.HashMap;
import java.util.Map;

public class IndexValueConverter {
    public static Object marshall(Object value) {
        if (value instanceof Geoshape) {
            return convertGeoshape((Geoshape) value);
        } else if (value instanceof Float) {
            return ((Float) value).doubleValue();
        }
        return value;
    }

    private static Object convertGeoshape(Geoshape value) {
        Map<String, Double> properties = new HashMap<>();
        Shape shape = value.getShape();
        if (shape instanceof Circle) {
            Circle circle = (Circle) shape;
            Point center = circle.getCenter();
            properties.put("lat", center.getLat());
            properties.put("lon", center.getLon());
            properties.put("radius", circle.getRadius());
        } else if (shape instanceof Rectangle) {
            Rectangle rect = (Rectangle) shape;
            properties.put("lat", rect.getMinX());
            properties.put("lon", rect.getMinY());
            properties.put("w", rect.getWidth());
            properties.put("h", rect.getHeight());
        } else if (shape instanceof Point) {
            Point point = (Point) shape;
            properties.put("lat", point.getLat());
            properties.put("lon", point.getLon());
        }
        return properties;
    }

}
