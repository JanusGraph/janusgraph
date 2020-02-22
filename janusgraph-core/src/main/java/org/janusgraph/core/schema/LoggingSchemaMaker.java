// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.core.schema;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.graphdb.tinkerpop.JanusGraphDefaultSchemaMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingSchemaMaker implements DefaultSchemaMaker {

	private static final Logger log = LoggerFactory.getLogger(LoggingSchemaMaker.class);

	public static final DefaultSchemaMaker DEFAULT_INSTANCE = new LoggingSchemaMaker(
			JanusGraphDefaultSchemaMaker.INSTANCE);

	private final DefaultSchemaMaker proxy;

	public LoggingSchemaMaker(DefaultSchemaMaker proxy) {
		this.proxy = proxy;
	}

	@Override
	public Cardinality defaultPropertyCardinality(String key) {
		return proxy.defaultPropertyCardinality(key);
	}

	@Override
	public boolean ignoreUndefinedQueryTypes() {
		return proxy.ignoreUndefinedQueryTypes();
	}

	@Override
	public EdgeLabel makeEdgeLabel(EdgeLabelMaker factory) {
		log.warn("Edge Label with given name does not exist: {}", factory.getName());
		return proxy.makeEdgeLabel(factory);
	}

	@Override
	public PropertyKey makePropertyKey(PropertyKeyMaker factory) {
		log.warn("Property Key with given name does not exist: {}", factory.getName());
		return proxy.makePropertyKey(factory);
	}

	@Override
	public PropertyKey makePropertyKey(PropertyKeyMaker factory, Object value) {
		log.warn("Property Key with given name does not exist: {}", factory.getName());
		return proxy.makePropertyKey(factory, value);
	}

	@Override
	public VertexLabel makeVertexLabel(VertexLabelMaker factory) {
		log.warn("Vertex Label with given name does not exist: {}", factory.getName());
		return proxy.makeVertexLabel(factory);
	}

	@Override
	public void makePropertyConstraintForVertex(VertexLabel vertexLabel, PropertyKey key,
			SchemaManager manager) {
		log.warn(
				"Property Key constraint does not exist for given Vertex Label {} and property key {}.",
				vertexLabel,
				key);
		proxy.makePropertyConstraintForVertex(vertexLabel, key, manager);
	}

	@Override
	public void makePropertyConstraintForEdge(EdgeLabel edgeLabel, PropertyKey key,
			SchemaManager manager) {
		log.warn(
				"Property Key constraint does not exist for given Edge Label {} and property key {}.",
				edgeLabel,
				key);
		proxy.makePropertyConstraintForEdge(edgeLabel, key, manager);
	}

	@Override
	public void makeConnectionConstraint(EdgeLabel edgeLabel, VertexLabel outVLabel,
			VertexLabel inVLabel, SchemaManager manager) {
		log.warn(
				"Connection constraint does not exist for given Edge Label {}, outgoing Vertex Label {} and incoming Vertex Label {}",
				edgeLabel,
				outVLabel,
				inVLabel);
		proxy.makeConnectionConstraint(edgeLabel, outVLabel, inVLabel, manager);
	}

}
