package com.thinkaurelius.titan.diskstorage.locking.consistentkey;


import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;

public class ExpiredLockException extends TemporaryLockingException {

    public ExpiredLockException(String msg) {
        super(msg);
    }

    public ExpiredLockException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public ExpiredLockException(Throwable cause) {
        super(cause);
    }
}
