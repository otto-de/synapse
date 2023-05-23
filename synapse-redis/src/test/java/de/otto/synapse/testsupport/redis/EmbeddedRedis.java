package de.otto.synapse.testsupport.redis;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import redis.embedded.RedisServer;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class EmbeddedRedis {

    private static final Logger LOG = getLogger(EmbeddedRedis.class);

    @Value("${spring.redis.port}")
    private int redisPort;

    private RedisServer redisServer;

    @PostConstruct
    public void startRedis() throws IOException {
        LOG.info("Starting embedded Redis server on port {}", redisPort);
        redisServer = new RedisServer(redisPort);
        redisServer.start();
    }

    @PreDestroy
    public void stopRedis() {
        LOG.info("Stopping embedded Redis server.");
        redisServer.stop();
    }
}