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

package org.janusgraph.diskstorage.solr.transform;

import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.diskstorage.BackendException;

public class GeoToWktConverter {
    /**
     * Get Well-Known Text format (e.g. POINT(X.0 Y.0)) for geoshape for indexing in solr.
     * Only points, lines and polygons are supported.
     */
    public static String convertToWktString(Geoshape fieldValue) throws BackendException {
        return fieldValue.toString();
    }

}
