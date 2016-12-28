package com.thinkaurelius.titan.graphdb.serializer.attributes;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum TEnum {

    ONE, TWO, THREE {
        @Override
        public String toString() {
            return "three";
        }
    }, FOUR;


    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }

}
