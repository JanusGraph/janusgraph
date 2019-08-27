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

package org.janusgraph.diskstorage.es.compat;

import org.janusgraph.diskstorage.indexing.IndexFeatures;

import static org.janusgraph.diskstorage.es.ElasticSearchConstants.CUSTOM_ALL_FIELD;

public class ES7Compat extends AbstractESCompat {

    private static final IndexFeatures FEATURES = coreFeatures().setWildcardField(CUSTOM_ALL_FIELD).supportsGeoContains().build();

    @Override
    public IndexFeatures getIndexFeatures() {
        return FEATURES;
    }
}
