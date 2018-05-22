package de.otto.synapse.info;

public enum MessageEndpointStatus {
    /**
     * Endpoint is initializing, but not yet running.
     */
    STARTING,
    /**
     * Endpoint is successfully initialized.
     */
    STARTED,
    /**
     * Endpoint is sending/receiving messages
     */
    RUNNING,
    /**
     * Endpoint has successfully finished
     */
    FINISHED,
    /**
     * Endpoint has finished with errors
     */
    FAILED
}
