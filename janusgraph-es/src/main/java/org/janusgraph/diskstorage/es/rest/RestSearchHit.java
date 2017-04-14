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

package org.janusgraph.diskstorage.es.rest;

import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown=true)
public class RestSearchHit {

    @JsonProperty("_index")
    private String index;

    @JsonProperty("_type")
    private String type;

    @JsonProperty("_id")
    private String id;

    @JsonProperty("_score")
    private Float score;

    @JsonProperty("_source")
    private Map<String,Object> source;

    private Map<String,List<Object>> fields;

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Float getScore() {
        return score;
    }

    public void setScore(Float score) {
        this.score = score;
    }

    public Map<String, Object> getSource() {
        return source;
    }

    public void setSource(Map<String, Object> source) {
        this.source = source;
    }

    public void setFields(Map<String, List<Object>> fields) {
        this.fields = fields;
    }

    public List<Object> field(String name) {
        return this.fields != null ? this.fields.get(name) : null;
    }

}
