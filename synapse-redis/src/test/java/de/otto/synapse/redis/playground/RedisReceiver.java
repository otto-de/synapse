package de.otto.synapse.redis.playground;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.CountDownLatch;

public class RedisReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisTemplate.class);

    private CountDownLatch latch;

    public RedisReceiver(final CountDownLatch latch) {
        this.latch = latch;
    }

    public void receiveMessage(final String message) {
        LOGGER.info("Received <" + message + ">");
        latch.countDown();
    }

    public void awaitMessage() throws InterruptedException {
        latch.await();
    }
}
