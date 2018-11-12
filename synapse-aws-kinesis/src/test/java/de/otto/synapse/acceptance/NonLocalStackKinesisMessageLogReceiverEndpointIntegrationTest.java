package de.otto.synapse.acceptance;


import de.otto.synapse.annotation.EnableMessageSenderEndpoint;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.selector.Kinesis;
import de.otto.synapse.configuration.kinesis.KinesisAutoConfiguration;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import de.otto.synapse.message.Message;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static de.otto.synapse.acceptance.NonLocalStackKinesisMessageLogReceiverEndpointIntegrationTest.IntegratedTestConfiguration.AWS_KINESIS_CHANNEL;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.synchronizedSet;
import static org.awaitility.Awaitility.waitAtMost;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.IsNot.not;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.synapse.acceptance"})
@SpringBootTest(classes = NonLocalStackKinesisMessageLogReceiverEndpointIntegrationTest.class)
@EnableMessageSenderEndpoint(channelName = AWS_KINESIS_CHANNEL, selector = Kinesis.class)
@DirtiesContext

public class NonLocalStackKinesisMessageLogReceiverEndpointIntegrationTest {

    @Configuration
    @ImportAutoConfiguration(KinesisAutoConfiguration.class)
    static class IntegratedTestConfiguration {

        static final String AWS_KINESIS_CHANNEL = "promo-productfeed-develop";

        @Bean
        public AwsCredentialsProvider awsCredentialsProvider() {
            return ProfileCredentialsProvider
                    .builder()
                    .profileName("ft3-nonlive")
                    .build();
        }

    }

    @Autowired
    private MessageLogReceiverEndpointFactory endpointFactory;

    private List<Message<String>> messages = synchronizedList(new ArrayList<>());
    private Set<String> threads = synchronizedSet(new HashSet<>());
    private MessageLogReceiverEndpoint kinesisMessageLog;

    @Before
    public void before() {
        /* We have to setup the EventSource manually, because otherwise the stream created above is not yet available
           when initializing it via @EnableEventSource
         */
        kinesisMessageLog = endpointFactory.create(AWS_KINESIS_CHANNEL);
        kinesisMessageLog.register(MessageConsumer.of(".*", String.class, (message) -> {
            messages.add(message);
            threads.add(Thread.currentThread().getName());
        }));

    }

    @After
    public void after() {
        kinesisMessageLog.stop();
    }

    //@Test
    public void consumeDataFromKinesis() {
        // given
        final ChannelPosition startFrom = fromHorizon();

        // when
        kinesisMessageLog.consumeUntil(
                startFrom,
                now().plus(200, MILLIS)
        );

        waitAtMost(Duration.TEN_SECONDS).until(() -> messages.size() > 100);

        kinesisMessageLog.stop();

        // then
        assertThat(messages, not(empty()));
    }

}
