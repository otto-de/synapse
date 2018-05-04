package de.otto.synapse.logging;

import org.slf4j.Logger;

public class ProgressLogger {

    private long percentageCount = 0L;
    private long currentCount = 0L;

    private final Logger logger;
    private final long expectedCount;

    public ProgressLogger(final Logger logger,
                          final long expectedCount) {
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
