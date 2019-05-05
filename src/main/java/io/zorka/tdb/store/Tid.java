package io.zorka.tdb.store;

import com.jitlogic.zorka.common.util.ZorkaUtil;

/** Represents trace ID or span ID or a single chunk */
public class Tid {
    public final long t1;

    public final long t2;

    public final long s;

    public final long c;

    private Tid(long t1, long t2, long s, int c) {
        this.t1 = t1;
        this.t2 = t2;
        this.s = s;
        this.c = c;
    }

    public static Tid t(long t1, long t2) {
        return new Tid(t1, t2, 0L, 0);
    }

    public static Tid s(long t1, long t2, long s) {
        return new Tid(t1, t2, s, 0);
    }

    public static Tid c(long t1, long t2, long s, int c) {
        return new Tid(t1, t2, s, c);
    }

    @Override
    public int hashCode() {
        return (int)(31*t1 + 17*t2 + 11*s + 3*c);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Tid)) return false;
        Tid t = (Tid)obj;
        return t.t1==t1 && t.t2==t2 && t.s==s && t.c==c;
    }

    @Override
    public String toString() {
        if (c != 0) return String.format("tid(%s,%s,%d)", ZorkaUtil.hex(t1,t2), ZorkaUtil.hex(s), c);
        if (s != 0) return String.format("tid(%s,%s)", ZorkaUtil.hex(t1,t2), ZorkaUtil.hex(s));
        return String.format("tid(%s)", ZorkaUtil.hex(t1,t2));
    }
}
