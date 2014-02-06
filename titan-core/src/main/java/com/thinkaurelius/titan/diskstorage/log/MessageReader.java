package com.thinkaurelius.titan.diskstorage.log;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface MessageReader {

    public void read(Message message);

    /**
     * Need to override this method because the {@link Log} uses this comparison
     * when unregistering readers
     *
     * @param other other reader to compare against
     * @return
     */
    @Override
    public boolean equals(Object other);

}
