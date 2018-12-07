package io.zorka.tdb.text;

import java.util.HashMap;
import java.util.Map;

public class StringCache {

    private int limit;

    private Map<String,Node> cache;

    private Node lru;
    private Node mru;

    public StringCache(int limit) {
        this.limit = limit;
        cache = new HashMap<>(limit);
    }

    public synchronized void add(int id, String val) {
        if (val == null || val.length() == 0) return;

        Node tmp = cache.get(val);

        if (tmp != null) {
            tmp.id = id;
            promote(tmp);
        } else {
            tmp = new Node(id, val);
            cache.put(val, tmp);
            if (mru == null) {
                mru = lru = tmp;
            } else {
                tmp.next = mru;
                mru.prev = tmp;
                tmp.prev = null;
                mru = tmp;
            }
        }

        // Remove least recently used pair if needed
        while (cache.size() > limit) {
            cache.remove(lru.val);
            lru = lru.prev;
            lru.next = null;
        }
    }

    public synchronized int get(String val) {
        if (val == null || val.length() == 0) return -1;

        Node tmp = cache.get(val);

        if (tmp == null) return -1;
        if (mru == tmp) return tmp.id;
        promote(tmp);


        return tmp.id;
    }

    private void promote(Node tmp) {
        // Remove tmp from current position in list

        if (tmp == mru) return;

        if (tmp.prev != null) {
            tmp.prev.next = tmp.next;
        }

        if (tmp.next != null) {
            tmp.next.prev = tmp.prev;
        }

        // Check and remove if it is on LRU position
        if (tmp == lru) lru = tmp.prev;

        // Add tmp back as MRU
        tmp.next = mru;
        mru.prev = tmp;
        tmp.prev = null;
        mru = tmp;
    }

    private static class Node {
        int id;
        String val;
        Node next;
        Node prev;

        Node(int id, String val) {
            this.id = id;
            this.val = val;
        }

        @Override
        public String toString() {
            return "(" + id +",'" + val +"')";
        }
    } // Node


} // StringCache
