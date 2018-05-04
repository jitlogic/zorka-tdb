package io.zorka.tdb.store;

import java.util.List;

public interface StoreSearchExprBuilder {

    Object stringToken(String s, boolean exact);

    Object regexToken(Object s);

    Object functionToken(String fn, List<Object> args);
}
