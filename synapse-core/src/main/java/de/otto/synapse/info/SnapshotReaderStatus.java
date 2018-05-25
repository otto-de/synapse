package de.otto.synapse.info;

public enum SnapshotReaderStatus {
    /**
     * Start retrieving snapshot data.
     */
    STARTING,
    /**
     * Start reading snapshot.
     */
    STARTED,
    /**
     * Reading snapshot successfully finished
     */
    FINISHED,
    /**
     * Reading snapshot has finished with errors
     */
    FAILED
}
