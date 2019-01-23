package de.otto.synapse.endpoint.sender.kinesis;

public class RetryLimitExceededException extends RuntimeException {

    private final int limit;

    public RetryLimitExceededException(String message, int limit) {
        super(message);
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }
}
