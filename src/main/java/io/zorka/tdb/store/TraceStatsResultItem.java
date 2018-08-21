package io.zorka.tdb.store;

public class TraceStatsResultItem {

    private int mid = 0;
    private int recs = 0;
    private int errors = 0;
    private long sumDuration = 0;
    private long maxDuration = 0;
    private long minDuration = Long.MAX_VALUE;

    private String method;

    public TraceStatsResultItem(TraceRecord tr) {
        this.mid = tr.getMid();
        add(tr);
    }

    public void add(TraceRecord tr) {
        this.recs++;
        this.sumDuration += tr.getDuration();
        this.minDuration = Math.min(this.minDuration, tr.getDuration());
        this.maxDuration = Math.max(this.maxDuration, tr.getDuration());
        if (tr.hasError()) {
            errors++;
        }
    }

    public int getMid() {
        return mid;
    }

    public int getRecs() {
        return recs;
    }

    public int getErrors() {
        return errors;
    }

    public long getSumDuration() {
        return sumDuration;
    }

    public long getMaxDuration() {
        return maxDuration;
    }

    public long getMinDuration() {
        return minDuration;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }
}
