package io.zorka.tdb.text;


/**
 * Contains some common elements for all text indexes: index state,
 */
public abstract class AbstractTextIndex implements TextIndex {

    private volatile TextIndexState state = TextIndexState.OPEN;
    private volatile long removalTime;

    public TextIndexState getState() {
        return state;
    }

    public synchronized void setState(TextIndexState state) {
        this.state = state;
    }

    public boolean canRemove() {
        return TextIndexState.REMOVAL.equals(state) && System.currentTimeMillis() > removalTime;
    }

    public synchronized void markForRemoval(long timeout) {
        state = TextIndexState.REMOVAL;
        removalTime = System.currentTimeMillis() + timeout;
    }

}
