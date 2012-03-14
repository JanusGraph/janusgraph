package com.thinkaurelius.titan.net;

import com.thinkaurelius.titan.net.msg.Query;

/**
 * Nodes attempting to answer a query instance store coarse-grained
 * progress timestamps for the instance in this object. 
 *
 * @author dalaro
 */
public class InstanceWorklog {
    private final Query query;
    private Long arrivalTime;
    private Long holdTime;
    private Long unholdTime;
    private Long runqueueTime;
    private Long startTime;
    private Long finishTime;

    public InstanceWorklog(Query query) {
        this.query = query;
    }
    
    synchronized void arrived(Long time)   { this.arrivalTime  = time; }
    synchronized void held(Long time)      { this.holdTime     = time; }
    synchronized void unheld(Long time)    { this.unholdTime   = time; }
    synchronized void runqueued(Long time) { this.runqueueTime = time; }
    synchronized void started(Long time)   { this.startTime    = time; }
    synchronized void finished(Long time)  { this.finishTime   = time; }
    
    public synchronized Long getArrivalTime()  { return arrivalTime;  }
    public synchronized Long getHoldTime()     { return holdTime;     }
    public synchronized Long getUnholdTime()   { return unholdTime;   }
    public synchronized Long getRunqueueTime() { return runqueueTime; }
    public synchronized Long getStartTime()    { return startTime;    }
    public synchronized Long getFinishTime()   { return finishTime;   }
    
    public Query getQuery() {
        return query;
    }
}
