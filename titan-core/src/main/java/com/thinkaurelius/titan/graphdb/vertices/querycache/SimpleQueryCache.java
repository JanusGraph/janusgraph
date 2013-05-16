package com.thinkaurelius.titan.graphdb.vertices.querycache;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleQueryCache implements QueryCache {

    private static final Logger log = LoggerFactory.getLogger(SimpleQueryCache.class);

    private static final int INITIAL_SIZE=5;
    private static final int MAX_SIZE=40;
    private static final int MULTIPLIER=2;

    private SliceQuery[] queries;
    private long[] time;

    public SimpleQueryCache() {
        queries=new SliceQuery[INITIAL_SIZE];
        time=new long[INITIAL_SIZE];
    }


    @Override
    public boolean isCovered(SliceQuery query) {
        Preconditions.checkNotNull(query);
        for (int i=0;i<queries.length;i++) {
            if(queries[i]!=null) {
                if (queries[i].subsumes(query)) {
                    time[i]=System.currentTimeMillis();
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean add(SliceQuery query) {
        Preconditions.checkNotNull(query);
        int position = -1;
        for (int i=0;i<queries.length;i++) {
            if (position<0 && (queries[i]==null || query.subsumes(queries[i]))) {
                position=i;
                queries[i]=query;
                time[i]=System.currentTimeMillis();
            } else if (queries[i]!=null && query.subsumes(queries[i])) {
                queries[i]=null;
                time[i]=0;
            }
        }
        if (position<0) {
            if (queries.length<MAX_SIZE) {
                SliceQuery[] newqueries = new SliceQuery[queries.length*MULTIPLIER];
                System.arraycopy(queries,0,newqueries,0,queries.length);
                long[] newtime = new long[newqueries.length];
                System.arraycopy(time,0,newtime,0,time.length);
                position=queries.length;
                queries=newqueries;
                time=newtime;
            } else {
                log.debug("Query Cache size at max [{}] - evicting least recently used",queries.length);
                long leastRecent = Long.MAX_VALUE;
                for (int i=0;i<queries.length;i++) {
                    assert queries[i]!=null;
                    if (leastRecent>time[i]) {
                        leastRecent=time[i];
                        position=i;
                    }
                }
            }
            Preconditions.checkArgument(position>=0);
            queries[position]=query;
            time[position]=System.currentTimeMillis();
        }
        return true;
    }
}
