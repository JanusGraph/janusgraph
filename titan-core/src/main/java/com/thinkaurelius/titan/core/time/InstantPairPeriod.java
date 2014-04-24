//package com.thinkaurelius.titan.core.time;
//
//import java.util.concurrent.TimeUnit;
//
//import com.google.common.base.Preconditions;
//
//public class InstantPairPeriod implements Period {
//
//
//    private final Timepoint begin;
//    private final Timepoint end;
//    private final TimeUnit computationUnit;
//
//    public InstantPairPeriod(final Timepoint begin, final Timepoint end, final TimeUnit computationUnit) {
//
//        if (0 <= begin.compareTo(end)) {
//            this.begin = begin;
//            this.end = end;
//        } else {
//            this.begin = begin;
//            this.end = this.begin;
//        }
//
//        this.computationUnit = computationUnit;
//
//        Preconditions.checkNotNull(this.computationUnit);
//        //Preconditions.checkArgument(0 > this.begin.compareTo(this.end));
//    }
//
//    @Override
//    public int compareTo(Duration o) {
//        final long mine = getLength(computationUnit);
//        final long theirs = o.getLength(computationUnit);
//        if (mine < theirs) {
//            return -1;
//        } else if (theirs < mine) {
//            return 1;
//        }
//        return 0;
//    }
//
//    @Override
//    public long getLength(TimeUnit unit) {
//        return end.getTime(unit) - begin.getTime(unit);
//    }
//
//    @Override
//    public Timepoint start() {
//        return begin;
//    }
//
//    @Override
//    public Timepoint end() {
//        return end;
//    }
//
//    @Override
//    public Period sub(Duration subtrahend) {
//
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public Period add(Duration addend) {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//}
