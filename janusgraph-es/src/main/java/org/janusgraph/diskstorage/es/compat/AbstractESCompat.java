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

package org.janusgraph.diskstorage.es.compat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.es.ElasticSearchRequest;
import org.janusgraph.diskstorage.indexing.IndexFeatures;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_ANALYZER;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_ID_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_LANG_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_PARAMS_FIELDS_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_PARAMS_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_SCRIPT_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_SOURCE_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_TYPE_KEY;

/**
 * Base class for building Elasticsearch mapping and query objects.
 */
public abstract class AbstractESCompat {

    static final Map<String,Object> MATCH_ALL = ImmutableMap.of("match_all", Collections.EMPTY_MAP);

    static IndexFeatures.Builder coreFeatures() {
        return new IndexFeatures.Builder()
            .setDefaultStringMapping(Mapping.TEXT)
            .supportedStringMappings(Mapping.TEXT, Mapping.TEXTSTRING, Mapping.STRING)
            .setWildcardField("_all")
            .supportsCardinality(Cardinality.SINGLE)
            .supportsCardinality(Cardinality.LIST)
            .supportsCardinality(Cardinality.SET)
            .supportsNanoseconds()
            .supportsCustomAnalyzer()
            .supportsGeoExists()
            .supportNotQueryNormalForm()
        ;
    }

    public abstract IndexFeatures getIndexFeatures();

    public Map<String,Object> createKeywordMapping() {
        return ImmutableMap.of(ES_TYPE_KEY, "keyword");
    }

    public Map<String,Object> createTextMapping(String textAnalyzer) {
        final ImmutableMap.Builder builder = ImmutableMap.builder().put(ES_TYPE_KEY, "text");
        return (textAnalyzer != null ? builder.put(ES_ANALYZER, textAnalyzer) : builder).build();
    }

    public String scriptLang() {
        return "painless";
    }

    public ImmutableMap.Builder<String, Object> prepareScript(String source) {
        Map<String, Object> script = ImmutableMap.of(ES_SOURCE_KEY, source,
            ES_LANG_KEY, scriptLang());
        return ImmutableMap.<String, Object>builder().put(ES_SCRIPT_KEY, script);
    }

    public ImmutableMap.Builder<String, Object> prepareStoredScript(String scriptId, List<Map<String, Object>> fields) {
        Map<String, Object> script = ImmutableMap.of(ES_ID_KEY, scriptId,
            ES_PARAMS_KEY, ImmutableMap.of(ES_PARAMS_FIELDS_KEY, fields));
        return ImmutableMap.<String, Object>builder().put(ES_SCRIPT_KEY, script);
    }

    public ImmutableMap.Builder<String, Object> prepareInlineScript(String source, List<Map<String, Object>> fields) {
        Map<String, Object> script = ImmutableMap.of(ES_SOURCE_KEY, source,
            ES_PARAMS_KEY, ImmutableMap.of(ES_PARAMS_FIELDS_KEY, fields),
            ES_LANG_KEY, scriptLang());
        return ImmutableMap.<String, Object>builder().put(ES_SCRIPT_KEY, script);
    }

    public Map<String,Object> prepareQuery(Map<String,Object> query) {
        return query;
    }

    public Map<String,Object> term(String key, Object value) {
        return ImmutableMap.of("term", ImmutableMap.of(key, value));
    }

    public Map<String,Object> contains(String key, List<String> terms) {
        return boolMust(terms.stream().map(term -> term(key, term)).collect(Collectors.toList()));
    }

    public Map<String,Object> boolMust(List<Map<String,Object>> queries) {
        return queries.size() > 1 ? ImmutableMap.of("bool", ImmutableMap.of("must", queries)) : queries.get(0);
    }

    public Map<String,Object> boolMustNot(Map<String,Object> query) {
        return ImmutableMap.of("bool", ImmutableMap.of("must_not", query));
    }

    public Map<String,Object> boolShould(List<Map<String,Object>> queries) {
        return ImmutableMap.of("bool", ImmutableMap.of("should", queries));
    }

    public Map<String,Object> boolFilter(Map<String,Object> query) {
        return ImmutableMap.of("bool", ImmutableMap.of("must", MATCH_ALL, "filter", query));
    }

    public Map<String,Object> lt(String key, Object value) {
        return ImmutableMap.of("range", ImmutableMap.of(key, ImmutableMap.of("lt", value)));
    }

    public Map<String,Object> lte(String key, Object value) {
        return ImmutableMap.of("range", ImmutableMap.of(key, ImmutableMap.of("lte", value)));
    }

    public Map<String,Object> gt(String key, Object value) {
        return ImmutableMap.of("range", ImmutableMap.of(key, ImmutableMap.of("gt", value)));
    }

    public Map<String,Object> gte(String key, Object value) {
        return ImmutableMap.of("range", ImmutableMap.of(key, ImmutableMap.of("gte", value)));
    }

    public Map<String,Object> prefix(String key, Object value) {
        return ImmutableMap.of("prefix", ImmutableMap.of(key, value));
    }

    public Map<String,Object> regexp(String key, Object value) {
        return ImmutableMap.of("regexp", ImmutableMap.of(key, value));
    }

    public Map<String,Object> exists(String key) {
        return ImmutableMap.of("exists", ImmutableMap.of("field", key));
    }

    public Map<String,Object> match(String key, Object value) {
        return match(key, value, null);
    }

    public Map<String,Object> matchPhrase(String key, Object value) {
        return ImmutableMap.of("match_phrase", ImmutableMap.of(key, value));
    }

    public Map<String,Object> fuzzyMatch(String key, Object value) {
        return match(key, value, "AUTO");
    }

    public Map<String,Object> match(String key, Object value, String fuzziness) {
        final ImmutableMap.Builder builder = ImmutableMap.builder().put("query", value);
        builder.put("operator", "and");
        if (fuzziness != null) builder.put("fuzziness", fuzziness);
        return ImmutableMap.of("match", ImmutableMap.of(key, builder.build()));
    }

    public Map<String,Object> queryString(Object query) {
        return ImmutableMap.of("query_string", ImmutableMap.of("query", query));
    }

    public Map<String,Object> geoDistance(String key, double lat, double lon, double radius) {
        return filter(ImmutableMap.of("geo_distance", ImmutableMap.of("distance", radius + "km", key, ImmutableList.of(lon, lat))));
    }

    public Map<String,Object> geoBoundingBox(String key, double minLat, double minLon, double maxLat, double maxLon) {
        return filter(ImmutableMap.of("geo_bounding_box", ImmutableMap.of(key,ImmutableMap.of(
            "top_left", ImmutableList.of(minLon, maxLat),"bottom_right", ImmutableList.of(maxLon, minLat)))));
    }

    public Map<String,Object> geoPolygon(String key, List<List<Double>> points) {
        return filter(ImmutableMap.of("geo_polygon", ImmutableMap.of(key, ImmutableMap.of("points", points))));
    }

    public Map<String,Object> geoShape(String key, Map<String,Object> geoShape, Geo predicate) {
        final String relation = predicate == Geo.INTERSECT ? "intersects" : predicate.name().toLowerCase();
        return filter(ImmutableMap.of("geo_shape", ImmutableMap.of(key, ImmutableMap.of("shape", geoShape, "relation", relation))));
    }

    public Map<String,Object> filter(Map<String,Object> query) {
        return boolFilter(query);
    }

    public Map<String,Object> createRequestBody(ElasticSearchRequest request, Parameter[] parameters) {
        final Map<String,Object> requestBody = createRequestBody(request.getQuery(), parameters);

        if(request.getSize() != null){
            requestBody.put("size", request.getSize());
        }

        if(request.getFrom() != null){
            requestBody.put("from", request.getFrom());
        }

        if (!request.getSorts().isEmpty()) {
            requestBody.put("sort", request.getSorts());
        }

        if(request.isDisableSourceRetrieval()){
            requestBody.put("_source", false);
        }

        return requestBody;
    }

    public Map<String,Object> createRequestBody(Map<String,Object> query, Parameter[] parameters) {
        final Map<String,Object> requestBody = new HashMap<>();

        if(query != null){
            requestBody.put("query", query);
        }

        if(parameters != null){
            for(Parameter parameter : parameters){
                requestBody.put(parameter.key(), parameter.value());
            }
        }

        return requestBody;
    }
}
