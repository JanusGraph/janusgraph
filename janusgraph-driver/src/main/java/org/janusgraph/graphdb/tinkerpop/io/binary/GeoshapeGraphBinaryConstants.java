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

package org.janusgraph.graphdb.tinkerpop.io.binary;

public class GeoshapeGraphBinaryConstants {

    // Geoshape format versions 0 and 1 were used by the legacy GeoshapeGraphBinarySerializer.
    public static final byte GEOSHAPE_FORMAT_VERSION = 2;

    // Geoshape type codes
    public static final int GEOSHAPE_POINT_TYPE_CODE = 0;
    public static final int GEOSHAPE_CIRCLE_TYPE_CODE = 1;
    public static final int GEOSHAPE_BOX_TYPE_CODE = 2;
    public static final int GEOSHAPE_LINE_TYPE_CODE = 3;
    public static final int GEOSHAPE_POLYGON_TYPE_CODE = 4;
    public static final int GEOSHAPE_MULTI_POINT_TYPE_CODE = 5;
    public static final int GEOSHAPE_MULTI_LINE_TYPE_CODE = 6;
    public static final int GEOSHAPE_MULTI_POLYGON_TYPE_CODE = 7;
    public static final int GEOSHAPE_GEOMETRY_COLLECTION_TYPE_CODE = 8;
}
