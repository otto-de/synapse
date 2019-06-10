package de.otto.synapse.logging;

import org.slf4j.Logger;
import org.slf4j.Marker;

public class ProgressLogger {

    private long percentageCount = 0L;
    private long currentCount = 0L;

    private final Logger logger;
    private final long expectedCount;

    private final Marker marker;

    public ProgressLogger(final Logger logger,
                          final long expectedCount,
                          final Marker marker) {
        this.logger = logger;
        this.expectedCount = expectedCount;
        this.marker = marker;
    }

    public void incrementAndLog() {
        currentCount++;
        long percentage = currentCount * 100 / expectedCount;
        if (percentage > percentageCount) {
            percentageCount++;
            logger.info(marker, "processed {}% of entries", percentage);
        }
    }

}
