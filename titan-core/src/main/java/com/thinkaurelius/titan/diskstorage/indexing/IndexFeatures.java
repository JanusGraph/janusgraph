package com.thinkaurelius.titan.diskstorage.indexing;

/**
 * Characterizes the features that a particular {@link IndexProvider} implementation supports
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IndexFeatures {

    private final boolean supportsDocumentTTL;

    public IndexFeatures(boolean supportsDocumentTTL) {
        this.supportsDocumentTTL = supportsDocumentTTL;
    }

    public boolean supportsDocumentTTL() {
        return supportsDocumentTTL;
    }

    public static class Builder {

        private boolean supportsDocumentTTL = false;

        public Builder supportsDocumentTTL() {
            supportsDocumentTTL=true;
            return this;
        }

        public IndexFeatures build() {
            return new IndexFeatures(supportsDocumentTTL);
        }


    }

}
