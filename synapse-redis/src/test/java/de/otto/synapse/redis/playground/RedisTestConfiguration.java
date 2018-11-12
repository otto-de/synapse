package de.otto.synapse.redis.playground;

import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

import java.util.concurrent.CountDownLatch;

import static org.slf4j.LoggerFactory.getLogger;

@Configuration
class RedisTestConfiguration {

    private static final Logger LOG = getLogger(RedisTestConfiguration.class);

    @Bean
    RedisReceiver redisReceiver() {
        return new RedisReceiver(new CountDownLatch(1));
    }

    @Bean
    MessageListenerAdapter listenerAdapter(final RedisReceiver receiver) {
        return new MessageListenerAdapter(receiver, "receiveMessage");
    }

    @Bean
    RedisMessageListenerContainer container(final RedisConnectionFactory connectionFactory) {

        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(
                listenerAdapter(redisReceiver()),
                new PatternTopic("redis-example-channel")
        );

        return container;
    }

}
