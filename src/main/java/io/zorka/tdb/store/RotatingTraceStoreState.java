package io.zorka.tdb.store;

import java.util.*;
import java.util.stream.Collectors;

public class RotatingTraceStoreState {

    private final int baseId;
    private final List<SimpleTraceStore> archived;
    private final SimpleTraceStore current;

    public static final RotatingTraceStoreState EMPTY = new RotatingTraceStoreState(Collections.EMPTY_LIST, null, 0);

    public static RotatingTraceStoreState init(List<SimpleTraceStore> stores) {
        List<SimpleTraceStore> tmp = stores.stream()
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(SimpleTraceStore::getStoreId))
                .collect(Collectors.toList());
        SimpleTraceStore current = tmp.get(tmp.size()-1);

        tmp.remove(tmp.size()-1);

        int baseId = (int)(tmp.size() > 0 ? tmp.get(0).getStoreId() : current.getStoreId());

        return new RotatingTraceStoreState(tmp, current, baseId);
    }

    public static RotatingTraceStoreState extend(RotatingTraceStoreState orig, SimpleTraceStore cur) {
        List<SimpleTraceStore> tmp = new ArrayList<>(orig.archived.size()+1);
        tmp.addAll(orig.archived);
        if (orig.current != null) tmp.add(orig.current);
        return new RotatingTraceStoreState(tmp, cur, orig.baseId);
    }

    public static RotatingTraceStoreState reduce(RotatingTraceStoreState orig) {
        List<SimpleTraceStore> tmp = new ArrayList<>(orig.archived.size()+1);
        tmp.addAll(!orig.archived.isEmpty() ? orig.archived.subList(1, orig.archived.size()) : Collections.emptyList());
        return new RotatingTraceStoreState(tmp, orig.current,
                (int)(!orig.archived.isEmpty() ? tmp.get(0).getStoreId() : orig.current.getStoreId()));
    }

    public RotatingTraceStoreState(List<SimpleTraceStore> archived, SimpleTraceStore current, int baseId) {
        this.archived = Collections.unmodifiableList(archived);
        this.current = current;
        this.baseId = baseId;
    }

    public int getBaseId() {
        return baseId;
    }

    public SimpleTraceStore getCurrent() {
        return current;
    }

    public List<SimpleTraceStore> getArchived() {
        return archived;
    }

    public SimpleTraceStore oldest() {
            return archived.isEmpty() ? archived.get(0) : current;
    }

    public SimpleTraceStore newest() {
            return current != null ? current :
                    archived.isEmpty() ? null : archived.get(archived.size()-1);
    }

    public SimpleTraceStore get(int storeId) {
        if (current != null && current.getStoreId() == storeId) return current;
        for (SimpleTraceStore s : archived) {
            if (s.getStoreId() == storeId) return s;
        }
        return null;
    }

    public boolean isOpen() {
        return current != null || !archived.isEmpty();
    }
}
