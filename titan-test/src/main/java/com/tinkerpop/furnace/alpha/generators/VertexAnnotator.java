package com.tinkerpop.furnace.alpha.generators;

import java.util.Map;
import java.util.Map.Entry;

import com.tinkerpop.blueprints.Vertex;

/**
 * VertexAnnotator is used to assign properties to generated edges.
 * 
 * During synthetic network generation,
 * {@link #annotate(com.tinkerpop.blueprints.Vertex)} is called on each
 * processed vertex exactly once. Hence, an implementation of this interface can
 * assign arbitrary properties to this vertex.
 * 
 * @author Uwe L. Korn (uwelk@xhochy.com)
 */

public interface VertexAnnotator {

	/**
	 * Empty {@link VertexAnnotator}. Does not assign any properties
	 */
	public final VertexAnnotator NONE = new VertexAnnotator() {
		@Override
		public void annotate(Vertex vertex, Map<String, Object> context) {
			// Do nothing
		}
	};

	/**
	 * Assign every context entry to a property with the same name.
	 */
	public final VertexAnnotator COPY_CONTEXT = new VertexAnnotator() {
		@Override
		public void annotate(Vertex vertex, Map<String, Object> context) {
			for(Entry<String, Object> entry: context.entrySet()) {
				vertex.setProperty(entry.getKey(), entry.getValue());
			}
		}
	};

	/**
	 * An implementation of this method can assign properties to the vertex.
	 * This method is called once for each processed vertex.
	 * 
	 * @param vertex Processed vertex.
	 */
	public void annotate(Vertex vertex, Map<String, Object> context);

}
