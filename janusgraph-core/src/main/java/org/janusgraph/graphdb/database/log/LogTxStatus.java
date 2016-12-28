package com.thinkaurelius.titan.graphdb.database.log;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public enum LogTxStatus {

    PRECOMMIT, PRIMARY_SUCCESS, COMPLETE_SUCCESS, SECONDARY_SUCCESS, SECONDARY_FAILURE, USER_LOG;

    public boolean isPrimarySuccess() {
        return this==PRIMARY_SUCCESS || this==COMPLETE_SUCCESS;
    }

    public boolean isSuccess() {
        return this==PRIMARY_SUCCESS || this==COMPLETE_SUCCESS || this==SECONDARY_SUCCESS;
    }

}
