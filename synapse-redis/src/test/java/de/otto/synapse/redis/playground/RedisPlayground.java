package de.otto.synapse.redis.playground;

import de.otto.synapse.testsupport.redis.EmbededRedis;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ComponentScan(
        basePackages = {"de.otto.synapse.redis.playground"})
@SpringBootTest(
        properties = {
                "spring.redis.server=localhost",
                "spring.redis.port=6379"
        },
        classes = {
                RedisPlayground.class,
                EmbededRedis.class
        })

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class RedisPlayground {

    @Autowired
    private StringRedisTemplate redisTemplate;
    @Autowired
    private RedisReceiver redisReceiver;

    @Test
    public void shouldSendAndReceiveMessage() throws InterruptedException {
        redisTemplate.convertAndSend("redis-example-channel", "some message " + LocalDateTime.now());
        redisReceiver.awaitMessage();
    }

}
