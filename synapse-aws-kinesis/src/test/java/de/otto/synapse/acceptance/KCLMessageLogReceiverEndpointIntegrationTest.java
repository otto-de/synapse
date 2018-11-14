package de.otto.synapse.acceptance;


import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.annotation.EnableMessageSenderEndpoint;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.selector.Kinesis;
import de.otto.synapse.configuration.aws.AwsProperties;
import de.otto.synapse.configuration.kinesis.KinesisAutoConfiguration;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.kinesis.KCLMessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.message.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static de.otto.synapse.acceptance.KCLMessageLogReceiverEndpointIntegrationTest.IntegratedTestConfiguration.AWS_KINESIS_CHANNEL;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.message.Message.message;
import static java.lang.Thread.sleep;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.synchronizedSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.IsNot.not;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.synapse.acceptance"})
@SpringBootTest(classes = KCLMessageLogReceiverEndpointIntegrationTest.class, properties = "synapse.aws.profile=${AWS_PROFILE}")
@EnableMessageSenderEndpoint(name = "kinesisSender", channelName = AWS_KINESIS_CHANNEL, selector = Kinesis.class)
@DirtiesContext
public class KCLMessageLogReceiverEndpointIntegrationTest {

    @Configuration
    @ImportAutoConfiguration(KinesisAutoConfiguration.class)
    static class IntegratedTestConfiguration {

        static final String AWS_KINESIS_CHANNEL = "TestStream";

        @Bean
        public AwsCredentialsProvider awsCredentialsProvider(final @Value("${synapse.aws.profile}") String profile) {
            return AwsCredentialsProviderChain
                    .builder()
                    .credentialsProviders(
                            EnvironmentVariableCredentialsProvider.create(),
                            ProfileCredentialsProvider
                                    .builder()
                                    .profileName(profile)
                                    .build())
                    .build();
        }

    }

    private static final Logger LOG = getLogger(KCLMessageLogReceiverEndpointIntegrationTest.class);

    @Autowired
    private MessageSenderEndpoint kinesisSender;
    @Autowired
    private MessageInterceptorRegistry interceptorRegistry;
    @Autowired
    private KinesisAsyncClient kinesisAsyncClient;

    private List<Message<String>> messages = synchronizedList(new ArrayList<>());
    private Set<String> threads = synchronizedSet(new HashSet<>());
    private MessageLogReceiverEndpoint kinesisMessageLog1;
    private MessageLogReceiverEndpoint kinesisMessageLog2;

    @Before
    public void before() {
        /* We have to setup the EventSource manually, because otherwise the stream created above is not yet available
           when initializing it via @EnableEventSource
         */
        AwsProperties awsProperties = new AwsProperties();
        awsProperties.setRegion(Region.EU_CENTRAL_1.toString());
        kinesisMessageLog1 = new KCLMessageLogReceiverEndpoint(AWS_KINESIS_CHANNEL, interceptorRegistry, new ObjectMapper(), null, kinesisAsyncClient, awsProperties);
        kinesisMessageLog1.register(MessageConsumer.of(".*", String.class, (message) -> {
            LOG.info("Received message in receiver-log 1 {}", message.getKey());
            messages.add(message);
            threads.add(Thread.currentThread().getName());
        }));
//        kinesisMessageLog2 = new KCLMessageLogReceiverEndpoint(AWS_KINESIS_CHANNEL, interceptorRegistry, new ObjectMapper(), null, kinesisAsyncClient, awsProperties);
//        kinesisMessageLog2.register(MessageConsumer.of(".*", String.class, (message) -> {
//            LOG.info("Received message in receiver-log 2: {}", message.getKey());
//            messages.add(message);
//            threads.add(Thread.currentThread().getName());
//        }));

    }

    @After
    public void after() {
        kinesisMessageLog1.stop();
//        kinesisMessageLog2.stop();
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void consumeDataFromKinesis() {
        // given
        final ChannelPosition startFrom = fromHorizon();

        // when
        kinesisSender.send(message("foo", "foo-value")).join();
        kinesisSender.send(message("bar", "bar-value")).join();

        CompletableFuture.supplyAsync(() -> {
            return kinesisMessageLog1.consumeUntil(
                    startFrom,
                    now().plus(2000, MILLIS)
            ).join();
        });
        try {
            sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        CompletableFuture.supplyAsync(() -> {
//            return kinesisMessageLog2.consumeUntil(
//                    startFrom,
//                    now().plus(2000, MILLIS)
//            ).join();
//        });
//
//        //waitAtMost(Duration.TEN_SECONDS).until(() -> messages.size() >= 6);
//
//        try {
//            sleep(10000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        kinesisMessageLog1.stop();
//        kinesisMessageLog2.stop();

        // then
        assertThat(messages, not(empty()));
    }

}
