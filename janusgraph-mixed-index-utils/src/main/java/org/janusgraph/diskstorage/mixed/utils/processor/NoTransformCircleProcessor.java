// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.diskstorage.mixed.utils.processor;

import org.janusgraph.core.attribute.Geoshape;

/**
 * Circle processor which isn't transforming circle into Polygon, but instead keep the circle shape unchanged.
 * This implementation may be useful in situations when the user wants to control circle transformation logic on the
 * mixed index server side. <br>
 * For example, using
 * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest-circle-processor.html">ElasticSearch Circle Processor</a>
 * or any custom plugin.
 */
public class NoTransformCircleProcessor implements CircleProcessor {

    public static final String SHORTHAND = "noTransformation";

    @Override
    public Geoshape process(Geoshape circle) {
        return circle;
    }
}
