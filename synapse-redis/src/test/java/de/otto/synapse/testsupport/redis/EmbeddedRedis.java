package de.otto.synapse.testsupport.redis;

import com.redis.testcontainers.RedisContainer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class EmbeddedRedis {

    private static final Logger LOG = getLogger(EmbeddedRedis.class);

    @Value("${spring.data.redis.port}")
    private int redisPort;
    private RedisContainer redisContainer;

    @PostConstruct
    public void startRedis() {
        LOG.info("Starting embedded Redis server on port {}.", redisPort);
        redisContainer = new RedisContainer(DockerImageName.parse("redis:7"));
        redisContainer.setPortBindings(List.of(String.format("%d:6379", redisPort)));
        redisContainer.start();
    }

    @PreDestroy
    public void stopRedis() {
        LOG.info("Stopping embedded Redis server.");
        redisContainer.stop();
    }
}