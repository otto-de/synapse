package de.otto.synapse.leaderelection.redis;

import de.otto.synapse.testsupport.redis.EmbededRedis;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.*;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ComponentScan(
        basePackages = {"de.otto.synapse.leaderelection"})
@SpringBootTest(
        properties = {
                "spring.redis.server=localhost",
                "spring.redis.port=6379"
        },
        classes = {
                RedisLeaderElectionTest.class,
                EmbededRedis.class
        })

public class RedisLeaderElectionTest {

    @Autowired
    private RedissonClient redissonClient;

    @Test
    public void runAsyncIfLeader2() {
        final RedisLeaderElection leaderElection = new RedisLeaderElection(redissonClient);
        final CountDownLatch latch = new CountDownLatch(1);
        allOf(
            leaderElection.runAsyncIfLeader("runAsyncIfLeader2", latch::countDown),
            leaderElection.runAsyncIfLeader("runAsyncIfLeader2", latch::countDown)
        ).join();
        assertThat(latch.getCount(), is(0L));
    }

    @Test
    public void runAsyncIfLeader3() {
        final Executor someExecutor = Executors.newCachedThreadPool();
        final RedisLeaderElection leaderElection = new RedisLeaderElection(redissonClient);
        final CountDownLatch latch = new CountDownLatch(1);
        allOf(
                leaderElection.runAsyncIfLeader("runAsyncIfLeader2", latch::countDown, someExecutor),
                leaderElection.runAsyncIfLeader("runAsyncIfLeader2", latch::countDown, someExecutor)
        ).join();
        assertThat(latch.getCount(), is(0L));
    }

    @Test
    public void supplyAsyncIfLeader2() throws InterruptedException {
        final RedisLeaderElection leaderElection = new RedisLeaderElection(redissonClient);
        final CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<String> first = leaderElection.supplyAsyncIfLeader("supplyAsyncIfLeader2", () -> {
            latch.countDown();
            return "1";
        });
        CompletableFuture<String> second = leaderElection.supplyAsyncIfLeader("supplyAsyncIfLeader2", () -> {
            latch.countDown();
            return "2";
        });
        latch.await(1, TimeUnit.SECONDS);
        assertThat(latch.getCount(), is(0L));
        assertThat(first.join(), is("1"));
        assertThat(second.join(), is("2"));
    }

    @Test
    public void runIfLeader() {
        final RedisLeaderElection leaderElection = new RedisLeaderElection(redissonClient);
        final CountDownLatch latch = new CountDownLatch(1);
        leaderElection.runIfLeader("runAsyncIfLeader2", latch::countDown);
        assertThat(latch.getCount(), is(0L));
    }

    @Test
    public void supplyIfLeader() {
        final RedisLeaderElection leaderElection = new RedisLeaderElection(redissonClient);
        final CountDownLatch latch = new CountDownLatch(1);
        final String result = leaderElection.supplyIfLeader("runAsyncIfLeader2", () -> {
            latch.countDown();
            return "42";
        });
        assertThat(latch.getCount(), is(0L));
        assertThat(result, is("42"));

    }
}