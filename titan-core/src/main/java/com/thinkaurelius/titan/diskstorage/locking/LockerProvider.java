package com.thinkaurelius.titan.diskstorage.locking;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface LockerProvider {

    public Locker getLocker(String lockerName);

}
