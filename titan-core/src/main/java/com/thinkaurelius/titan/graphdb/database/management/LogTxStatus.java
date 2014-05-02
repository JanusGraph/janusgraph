package com.thinkaurelius.titan.graphdb.database.management;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public enum LogTxStatus {
    PREFLUSH_SYSTEM, SUCCESS_SYSTEM, FAILURE_SYSTEM,
    PRECOMMIT, SUCCESS, FAILURE;

    public boolean isPreCommit() {
        switch(this) {
            case PRECOMMIT:
            case PREFLUSH_SYSTEM:
                return true;
            default: return false;
        }
    }

    public boolean isSuccess() {
        switch(this) {
            case SUCCESS:
            case SUCCESS_SYSTEM:
                return true;
            default: return false;
        }
    }

    public boolean isFailure() {
        switch(this) {
            case FAILURE:
            case FAILURE_SYSTEM:
                return true;
            default: return false;
        }
    }
}
