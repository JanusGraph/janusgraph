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

import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticSearchRequest {

    private Map<String,Object> query;

    private Integer size;

    private Integer from;

    private final List<Map<String,RestSortInfo>> sorts;

    private List<String> fields;

    private boolean disableSourceRetrieval;

    public ElasticSearchRequest() {
        this.sorts = new ArrayList<>();
        this.fields = new ArrayList<>();
    }

    public Map<String,Object> getQuery() {
        return query;
    }

    public void setQuery(Map<String,Object> query) {
        this.query = query;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public Integer getFrom() {
        return from;
    }

    public void setFrom(Integer from) {
        this.from = from;
    }

    public List<Map<String,RestSortInfo>> getSorts() {
        return sorts;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public void addSort(String key, String order, String unmappedType) {
        this.sorts.add(ImmutableMap.of(key, new RestSortInfo(order, unmappedType)));
    }

    public boolean isDisableSourceRetrieval() {
        return disableSourceRetrieval;
    }

    public void setDisableSourceRetrieval(boolean disableSourceRetrieval) {
        this.disableSourceRetrieval = disableSourceRetrieval;
    }

    public static class RestSortInfo {

        String order;

        @JsonProperty("unmapped_type")
        String unmappedType;

        public RestSortInfo(String order, String unmappedType) {
            this.order = order;
            this.unmappedType = unmappedType;
        }

        public String getOrder() {
            return order;
        }

        public void setOrder(String order) {
            this.order = order;
        }

        public String getUnmappedType() {
            return unmappedType;
        }

        public void setUnmappedType(String unmappedType) {
            this.unmappedType = unmappedType;
        }

    }
}
