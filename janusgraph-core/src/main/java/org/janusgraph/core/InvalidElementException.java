package org.janusgraph.core;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * Exception thrown when an element is invalid for the executing operation or when an operation could not be performed
 * on an element.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class InvalidElementException extends JanusGraphException {

    private final JanusGraphElement element;

    /**
     * @param msg     Exception message
     * @param element The invalid element causing the exception
     */
    public InvalidElementException(String msg, JanusGraphElement element) {
        super(msg);
        this.element = element;
    }

    /**
     * Returns the element causing the exception
     *
     * @return The element causing the exception
     */
    public JanusGraphElement getElement() {
        return element;
    }

    @Override
    public String toString() {
        return super.toString() + " [" + element.toString() + "]";
    }

    public static IllegalStateException removedException(JanusGraphElement element) {
        Class elementClass = Vertex.class.isAssignableFrom(element.getClass())?Vertex.class:
                (Edge.class.isAssignableFrom(element.getClass())?Edge.class:VertexProperty.class);
        return Element.Exceptions.elementAlreadyRemoved(elementClass, element.id());
    }

}
