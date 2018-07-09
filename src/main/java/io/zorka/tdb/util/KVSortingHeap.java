package io.zorka.tdb.util;


/**
 * Bounded heap that keeps bounded number of (K,V) pairs sorted by V.
 */
public class KVSortingHeap {

    /** Inversion flag. When false, min elements are on top of heap, when true - max elements are on top of heap. */
    private boolean inv;

    private boolean heapified = false;

    private int size = 0, limit;
    private long[] data;

    public KVSortingHeap(int limit, boolean inv) {
        this.inv = inv;
        this.limit = limit;
        data = new long[limit+1];
    }

    public void add(int k, int v) {
        if (!heapified && size < limit) {
            size++;
            data[size] = kv(k, v);
            if (size == limit) {
                heapify();
            }
        } else {
            int vr = val(data[1]);
            if ((inv && v > vr) || (!inv && v < vr)) {
                data[1] = kv(k, v);
                heapify(1);
            }
        }
    } // add()


    public int next() {
        if (size > 0) {

            if (!heapified) heapify();

            int rslt = id(data[1]);

            data[1] = data[size];
            data[size] = -1;
            size--;

            heapify(1);

            return rslt;
        } else {
            return -1;
        }
    }


    public void invert() {
        inv = !inv;
        heapify();
    }


    /**
     * Restores heap properties for single item.
     * @param i element index
     */
    private void heapify(int i) {

        while (i <= size) {

            int l = l(i);
            int r = r(i);
            int v = val(data[i]);
            int j = i;

            if (inv) {
                // Min element first
                if (l <= size) {
                    int vl = val(data[l]);
                    if (vl < v) {
                        j = l;
                        v = vl;
                    }
                }
                if (r <= size && val(data[r]) < v) {
                    j = r;
                }
            } else {
                // Max element first
                if (l <= size) {
                    int vl = val(data[l]);
                    if (vl > v) {
                        j = l;
                        v = vl;
                    }
                }
                if (r <= size && val(data[r]) > v) {
                    j = r;
                }
            }

            if (j != i) {
                long tmp = data[i];
                data[i] = data[j];
                data[j] = tmp;
                i = j;
            } else {
                break;
            }
        }
    }

    /**
     * Heapifies all items.
     */
    public void heapify() {
        for (int i = limit/2; i > 0; i--) {
            heapify(i);
        }
        heapified = true;
    }


    public int size() {
        return size;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');

        for (int i = 1; i <= size; i++) {
            if (i > 1) sb.append(',');
            sb.append('<');
            sb.append(id(data[i]));
            sb.append(',');
            sb.append(val(data[i]));
            sb.append('>');
        }

        sb.append(']');
        return sb.toString();
    }


    private static int id(long l) {
        return (int)(l >>> 32);
    }


    private static int val(long l) {
        return (int)l;
    }


    private static long kv(int id, int val) {
        return val | (((long)id) << 32);
    }


    private static int l(int n) {
        return n * 2;
    }


    private static int r(int n) {
        return n * 2 + 1;
    }
}
