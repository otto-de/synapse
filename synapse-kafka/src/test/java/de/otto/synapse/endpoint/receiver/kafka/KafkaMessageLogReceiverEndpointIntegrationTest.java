package de.otto.synapse.endpoint.receiver.kafka;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.google.common.collect.Sets.newConcurrentHashSet;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.channel.StopCondition.endOfChannel;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.awaitility.Awaitility.waitAtMost;
import static org.awaitility.Duration.TEN_SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps;
import static org.springframework.kafka.test.utils.KafkaTestUtils.producerProps;

@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka(
        partitions = 2,
        topics = KafkaMessageLogReceiverEndpointIntegrationTest.KAFKA_TOPIC)
public class KafkaMessageLogReceiverEndpointIntegrationTest {
    public static final String KAFKA_TOPIC = "test-stream";

    @Autowired
    private KafkaTemplate<String, String> template;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private KafkaConsumer<String, String> kafkaConsumer;
    private KafkaMessageLogReceiverEndpoint endpoint;
    private ExecutorService executorService = newFixedThreadPool(2);

    @Before
    public void setUp() {
        final String groupId = "KafkaMessageLogReceiverEndpointTest";
        final Map<String, Object> configs = new HashMap<>(consumerProps(
                groupId,
                "true",
                embeddedKafkaBroker));
        configs.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10); // set to small value to enforce multiple reads from kafka
        kafkaConsumer = new KafkaConsumer<>(configs, new StringDeserializer(), new StringDeserializer());

        MessageInterceptorRegistry interceptorRegistry = new MessageInterceptorRegistry();
        endpoint = new KafkaMessageLogReceiverEndpoint(
                KAFKA_TOPIC,
                interceptorRegistry,
                kafkaConsumer,
                executorService,
                eventPublisher);
    }

    @After
    public void tearDown() {
        endpoint.stop();
        kafkaConsumer.close();
    }

    @Test
    public void shouldAssignPartitions() throws Exception {
        // given
        template.send(new ProducerRecord<>(KAFKA_TOPIC, "my-aggregate-id", "{\"event\":\"Test Event\"}"));

        // when
        final CompletableFuture<ChannelPosition> channelPosition = endpoint.consumeUntil(fromHorizon(), endOfChannel());

        // then
        waitAtMost(TEN_SECONDS).until(channelPosition::isDone);
        assertThat(channelPosition.get().shards().isEmpty(), is(false));
    }

    @Test
    public void shouldSubscribeToStream() {
        // given
        template.send(new ProducerRecord<>(KAFKA_TOPIC, "my-aggregate-id", "{\"event\":\"Test Event\"}"));

        // when
        final CompletableFuture<ChannelPosition> channelPosition = endpoint.consumeUntil(fromHorizon(), endOfChannel());

        // then
        waitAtMost(TEN_SECONDS).until(channelPosition::isDone);
        assertThat(kafkaConsumer.subscription(), contains("test-stream"));
    }

    @Test
    public void shouldProcessRecords() throws ExecutionException, InterruptedException {
        // given
        template.send(new ProducerRecord<>(KAFKA_TOPIC, "test-key", "{\"event\":\"Test Event\"}"));

        // when
        final Set<String> receivedMessageKeys = newConcurrentHashSet();
        endpoint.getMessageDispatcher().add(MessageConsumer.of(".*", String.class, m -> receivedMessageKeys.add(m.getKey().compactionKey())));
        final CompletableFuture<ChannelPosition> channelPosition = endpoint.consumeUntil(fromHorizon(), endOfChannel());

        // then
        waitAtMost(TEN_SECONDS).until(channelPosition::isDone);
        final ShardPosition shard = channelPosition.get().shard("0");
        assertThat(shard.startFrom(), is(StartFrom.POSITION));
        assertThat(receivedMessageKeys, is(not(empty())));
        assertThat(receivedMessageKeys.contains("test-key"), is(true));
    }

    @Test
    public void shouldStartFromHorizon() {
        // given
        for (int i = 0; i < 10; i++) {
            template.send(new ProducerRecord<>(KAFKA_TOPIC, "shouldStartFromHorizon" + i, "{\"event\":\"Test Event\"}"));
        }

        endpoint.consumeUntil(fromHorizon(), endOfChannel()).join();

        // when
        final List<String> receivedMessageKeys = new ArrayList<>();
        endpoint.getMessageDispatcher().add(MessageConsumer.of("shouldStartFromHorizon.*", String.class, m -> receivedMessageKeys.add(m.getKey().compactionKey())));
        final CompletableFuture<ChannelPosition> channelPosition = endpoint.consumeUntil(fromHorizon(), endOfChannel());

        // then
        waitAtMost(TEN_SECONDS).until(channelPosition::isDone);
        assertThat(receivedMessageKeys.subList(receivedMessageKeys.size() - 10, receivedMessageKeys.size()), containsInAnyOrder("shouldStartFromHorizon0", "shouldStartFromHorizon1", "shouldStartFromHorizon2", "shouldStartFromHorizon3", "shouldStartFromHorizon4", "shouldStartFromHorizon5", "shouldStartFromHorizon6", "shouldStartFromHorizon7", "shouldStartFromHorizon8", "shouldStartFromHorizon9"));
    }

    @Test
    public void shouldStartFromPosition() {
        // given
        for (int i = 0; i < 100; i++) {
            template.send(new ProducerRecord<>(KAFKA_TOPIC, "" + i, "{\"event\":\"Test Event\"}"));
        }

        endpoint.consumeUntil(fromHorizon(), endOfChannel()).join();

        // when
        final Set<String> receivedMessageKeys = newConcurrentHashSet();
        endpoint.getMessageDispatcher().add(MessageConsumer.of(".*", String.class, m -> receivedMessageKeys.add(m.getKey().compactionKey())));

        final ChannelPosition startFrom = channelPosition(fromPosition("0", "25"), fromPosition("1", "25"));
        final CompletableFuture<ChannelPosition> channelPosition = endpoint.consumeUntil(startFrom, endOfChannel());

        // then
        waitAtMost(TEN_SECONDS).until(channelPosition::isDone);
        assertThat(receivedMessageKeys.size(), is(48));
    }

    @Test
    public void shouldContinueFromPosition() throws ExecutionException, InterruptedException {
        // given
        for (int i = 0; i < 10; i++) {
            template.send(new ProducerRecord<>(KAFKA_TOPIC, "" + i, "{\"event\":\"Test Event\"}")).get();
        }

        ChannelPosition startFrom = endpoint.consumeUntil(fromHorizon(), endOfChannel()).join();

        // when
        for (int j = 0; j < 10; j++) {
            final List<String> receivedMessageKeys = new ArrayList<>();
            endpoint.getMessageDispatcher().add(MessageConsumer.of(".*", String.class, m -> receivedMessageKeys.add(m.getKey().compactionKey())));

            final CompletableFuture<ChannelPosition> channelPosition = endpoint.consumeUntil(startFrom, (_x) -> receivedMessageKeys.size() == 10);
            for (int i = 0; i < 10; i++) {
                final ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, "continued-" + j + "/" + i, "{\"event\":\"Test Event\"}");
                template.send(record).get();
            }

            // then
            waitAtMost(TEN_SECONDS).until(channelPosition::isDone);
            assertThat(receivedMessageKeys.size(), is(10));
            assertThat(receivedMessageKeys, containsInAnyOrder("continued-" + j + "/0", "continued-" + j + "/1", "continued-" + j + "/2", "continued-" + j + "/3", "continued-" + j + "/4", "continued-" + j + "/5", "continued-" + j + "/6", "continued-" + j + "/7", "continued-" + j + "/8", "continued-" + j + "/9"));
            startFrom = channelPosition.get();
        }
    }

    @Test
    public void shouldConsumeUntilBothPartitionsAreAtEnd() throws ExecutionException, InterruptedException {
        // given
        for (int i = 0; i < 5; i++) {  //send to partition 1 and 2 alternating
            template.send(new ProducerRecord<>(KAFKA_TOPIC, i % 2, "" + i, "{\"event\":\"Test Event\"}")).get();
        }
        for (int i = 10; i < 20; i++) { //only send to partition 1, so that this partition has more entries than the other
            template.send(new ProducerRecord<>(KAFKA_TOPIC, 1, "" + i, "{\"event\":\"Test Event\"}")).get();
        }

        //when
        ChannelPosition channelPosition = endpoint.consumeUntil(fromHorizon(), endOfChannel()).join();

        //then
        List<TopicPartition> topicPartitions = kafkaConsumer.listTopics().get(KAFKA_TOPIC).stream().map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition())).collect(Collectors.toList());
        Map<TopicPartition, Long> topicPartitionOffsets = kafkaConsumer.endOffsets(topicPartitions);
        topicPartitionOffsets.forEach((topicPartition, offset) -> assertThat(channelPosition.shard("" + topicPartition.partition()).position(), is("" + (offset - 1))));
    }

    @Configuration
    @EnableKafka
    public static class TestConfiguration {


        @Bean
        public ProducerFactory<String, String> producerFactory(final EmbeddedKafkaBroker embeddedKafkaBroker) {
            final Map<String, Object> configs = producerProps(embeddedKafkaBroker);
            configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            return new DefaultKafkaProducerFactory<>(configs);
        }

        @Bean
        public KafkaTemplate<String, String> kafkaTemplate(final ProducerFactory<String, String> producerFactory) {
            return new KafkaTemplate<>(producerFactory);
        }
    }

}
