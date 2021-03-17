package de.otto.synapse.redis.playground;

import de.otto.synapse.leaderelection.LeaderElection;
import de.otto.synapse.leaderelection.redis.RedisLeaderElection;
import de.otto.synapse.testsupport.redis.EmbeddedRedis;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.redisson.api.RList;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ComponentScan(
        basePackages = {"de.otto.synapse.redis.playground"})
@SpringBootTest(
        properties = {
                "spring.redis.server=localhost",
                "spring.redis.port=6479"
        },
        classes = {
                RedisPlayground.class,
                EmbeddedRedis.class
        })

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class RedisPlayground {

    private static final Logger LOG = getLogger(RedisPlayground.class);

    @Autowired
    private StringRedisTemplate redisTemplate;
    @Autowired
    private RedisReceiver redisReceiver;
    @Autowired
    private RedissonClient redisson;

    @Test
    public void shouldSendAndReceiveMessage() throws InterruptedException {
        redisTemplate.convertAndSend("redis-example-channel", "some message " + LocalDateTime.now());
        redisReceiver.awaitMessage();
    }

    @Test
    public void shouldRetrieveMessagesByPosition() {
        RList<String> list = redisson.getList("test-list");
        for (int i = 0; i < 10; i++) {
            list.add("message-" + i);
        }
        list.listIterator(5).forEachRemaining((m) -> {
            LOG.info("Received message={}", m);
        });
    }

    //@Test
    public void shouldBuildReallyLargeList() {
        RList<String> list = redisson.getList("test-list");
        for (int i = 0; i < 1000000; i++) {
            list.add("message-" + i);
        }
        list.listIterator(999995).forEachRemaining((m) -> {
            LOG.info("Received message={}", m);
        });
    }

    @Test
    public void shouldRetrieveMessagesFromTopic() {
        RTopic topic = redisson.getTopic("test-topic");
        topic.addListener(String.class, (channel, message) -> {
            LOG.info("Received message={} from channel={}", message, channel);
        });
        topic.publish("some message");
        topic.removeAllListeners();
    }

    @Test
    public void shouldRunIfMaster() {
        final LeaderElection leaderElection = new RedisLeaderElection(redisson);
        CompletableFuture
                .allOf(
                        leaderElection.runAsyncIfLeader("master-slave-test-lock", something()),
                        leaderElection.runAsyncIfLeader("master-slave-test-lock", something()),
                        leaderElection.runAsyncIfLeader("master-slave-test-lock", something())
                )
                .join();
    }

    private Runnable something() {
        return () -> {
            try {
                for (int i = 0; i < 10; i++) {
                    LOG.info("Thread " + Thread.currentThread().getName() + " is master");
                    Thread.sleep(1000);
                }
            } catch(InterruptedException e){
                LOG.error(e.getMessage(), e);
            }
        };
    }

}
