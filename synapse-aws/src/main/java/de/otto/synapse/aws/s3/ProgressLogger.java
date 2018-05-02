package de.otto.synapse.aws.s3;

import org.slf4j.Logger;

public class ProgressLogger {

    private long percentageCount = 0L;
    private long currentCount = 0L;

    private final Logger logger;
    private final long expectedCount;

    ProgressLogger(Logger logger, long expectedCount) {
        this.logger = logger;
        this.expectedCount = expectedCount;
    }

    public void incrementAndLog() {
        currentCount++;
        long percentage = currentCount * 100 / expectedCount;
        if (percentage > percentageCount) {
            percentageCount++;
            logger.info("processed {}% of entries", percentage);
        }
    }

}
