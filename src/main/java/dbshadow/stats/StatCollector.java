package dbshadow.stats;

public class StatCollector {
    private long initialFetchTime = -1L;

    public StatCollector() {
    }

    public void startInitialFetch() {
        initialFetchTime = System.currentTimeMillis();
    }

    public void completeInitialFetch() {
        initialFetchTime = System.currentTimeMillis() - initialFetchTime;
    }

    public long getInitialFetchTime() {
        return initialFetchTime;
    }

    public void tableCreated() {

    }

    public void tableDropped() {

    }

    public void tableTruncated() {

    }

    public void recordInserted() {

    }

    public void recordUpdated() {

    }

    public void recordDeleted() {

    }

    public void commit() {

    }

    public void done() {

    }
}
