package de.otto.edison.eventsourcing.aws.kinesis;

public enum Status {
    /** Successfully retrieved 0-N messages from Kinesis. */
    OK,
    /** Service is shutting down or a stop-condition was met. */
    STOPPED;
}
