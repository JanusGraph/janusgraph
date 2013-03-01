package com.thinkaurelius.titan.core;

/**
 * Exception thrown when an element is invalid for the executing operation or when an operation could not be performed
 * on an element.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class InvalidElementException extends TitanException {

    private final TitanElement element;

    /**
     * @param msg     Exception message
     * @param element The invalid element causing the exception
     */
    public InvalidElementException(String msg, TitanElement element) {
        super(msg);
        this.element = element;
    }

    /**
     * Returns the element causing the exception
     *
     * @return The element causing the exception
     */
    public TitanElement getElement() {
        return element;
    }

    @Override
    public String toString() {
        return super.toString() + " [" + element.toString() + "]";
    }

}
