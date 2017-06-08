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

package org.janusgraph.diskstorage.es;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.janusgraph.core.JanusGraphFactory;

public class ElasticSearchConstants {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchConstants.class);

    public static final String ES_PROPERTIES_FILE = "janusgraph-es.properties";
    public static final String ES_DOC_KEY = "doc";
    public static final String ES_UPSERT_KEY = "upsert";
    public static final String ES_SCRIPT_KEY = "script";
    public static final String ES_INLINE_KEY = "inline";
    public static final String ES_LANG_KEY = "lang";
    public static final String ES_TYPE_KEY = "type";
    public static final String ES_INDEX_KEY = "index";
    public static final String ES_ANALYZER = "analyzer";
    public static final String ES_GEO_COORDS_KEY = "coordinates";
}
