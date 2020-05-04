package de.otto.synapse.endpoint.sender.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.translator.MessageTranslator;
import de.otto.synapse.translator.TextMessageTranslator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.of;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.getLast;
import static de.otto.synapse.endpoint.MessageInterceptorRegistration.matchingSenderChannelsWith;
import static de.otto.synapse.message.Message.message;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME;
import static org.springframework.kafka.test.EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS;
import static org.springframework.kafka.test.utils.KafkaTestUtils.*;

@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka(
        partitions = 1,
        topics = KafkaMessageSenderTest.KAFKA_TOPIC)
public class KafkaMessageSenderTest {

    public static final String KAFKA_TOPIC = "test-stream";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;
    @Autowired
    private KafkaTemplate<String, String> template;

    private MessageTranslator<TextMessage> messageTranslator = new TextMessageTranslator();
    private MessageInterceptorRegistry interceptorRegistry;
    private KafkaMessageSender messageSender;

    @Before
    public void setUp() {

        interceptorRegistry = new MessageInterceptorRegistry();
        messageSender = new KafkaMessageSender(KAFKA_TOPIC, interceptorRegistry, messageTranslator, template);
    }

    @Test
    public void shouldSendEvent() {
        // given
        final Message<ExampleJsonObject> message = message("someKey", new ExampleJsonObject("banana"));

        try (final Consumer<String, String> consumer = getKafkaConsumer("someTestGroup")) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, KAFKA_TOPIC);

            // when
            messageSender.send(message).join();

            // then

            final ConsumerRecord<String, String> record = getSingleRecord(consumer, KAFKA_TOPIC, 250L);
            assertThat(record.key(), is("someKey"));
            assertThat(record.value(), is("{\"value\":\"banana\"}"));
            assertThat(record.headers(), containsInAnyOrder(
                    new RecordHeader("_synapse_msg_partitionKey", "someKey".getBytes(UTF_8)),
                    new RecordHeader("_synapse_msg_compactionKey", "someKey".getBytes(UTF_8))
            ));
            assertThat(record.topic(), is(KAFKA_TOPIC));
            assertThat(record.partition(), is(0));
        }
    }

    @Test
    public void shouldInterceptMessages() {
        // given
        final Message<ExampleJsonObject> message = message("someKey", new ExampleJsonObject("banana"));

        try (final Consumer<String, String> consumer = getKafkaConsumer("someTestGroup")) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, KAFKA_TOPIC);

            // and especially
            interceptorRegistry.register(matchingSenderChannelsWith(
                    KAFKA_TOPIC,
                    (m) -> TextMessage.of(m.getKey(), m.getHeader(), "{\"value\" : \"apple\"}"))
            );

            // when
            messageSender.send(message).join();

            // then
            final ConsumerRecord<String, String> record = getSingleRecord(consumer, KAFKA_TOPIC, 250L);
            assertThat(record.key(), is("someKey"));
            assertThat(record.value(), is("{\"value\" : \"apple\"}"));
        }
    }

    @Test
    public void shouldNotSendMessagesDroppedByInterceptor() {
        // given
        final Message<ExampleJsonObject> message = message("someDroppedMessageKey", new ExampleJsonObject("banana"));

        try (final Consumer<String, String> consumer = getKafkaConsumer("someTestGroup")) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, KAFKA_TOPIC);

            // and especially
            interceptorRegistry.register(matchingSenderChannelsWith(
                    KAFKA_TOPIC,
                    (m) -> null)
            );

            // when
            messageSender.send(message).join();

            // then
            assertThat(getRecords(consumer, 200).isEmpty(), is(true));
        }
    }

    @Test
    public void shouldSendBatch() throws Exception {
        // given
        final ExampleJsonObject appleObject = new ExampleJsonObject("apple");
        final ExampleJsonObject bananaObject = new ExampleJsonObject("banana");

        try (final Consumer<String, String> consumer = getKafkaConsumer("someTestGroup")) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, KAFKA_TOPIC);

            // when
            messageSender.sendBatch(Stream.of(
                    message("a", appleObject),
                    message("b", bananaObject)
            ));

            // then
            final ConsumerRecords<String, String> records = getRecords(consumer, 250L, 2);
            assertThat(records.count(), is(2));

            final ConsumerRecord<String, String> first = getFirst(records.records(KAFKA_TOPIC), null);
            assertThat(first.key(), is("a"));
            assertThat(first.value(), is("{\"value\":\"apple\"}"));

            final ConsumerRecord<String, String> second = getLast(records.records(KAFKA_TOPIC), null);
            assertThat(second.key(), is("b"));
            assertThat(second.value(), is("{\"value\":\"banana\"}"));
        }
    }

    @Test
    public void shouldInterceptMessagesInBatch() throws Exception {
        // given
        final ExampleJsonObject appleObject = new ExampleJsonObject("apple");
        final ExampleJsonObject bananaObject = new ExampleJsonObject("banana");

        try (final Consumer<String, String> consumer = getKafkaConsumer("someTestGroup")) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, KAFKA_TOPIC);

            // and especially
            interceptorRegistry.register(matchingSenderChannelsWith(
                    KAFKA_TOPIC,
                    (m) -> TextMessage.of(m.getKey(), m.getHeader(), "{\"value\" : \"Lovely day for a Guinness\"}"))
            );

            // when
            messageSender.sendBatch(Stream.of(
                    message("a", appleObject),
                    message("b", bananaObject)
            ));

            // then
            final ConsumerRecords<String, String> records = getRecords(consumer, 250L, 2);
            assertThat(records.count(), is(2));

            final ConsumerRecord<String, String> first = getFirst(records.records(KAFKA_TOPIC), null);
            assertThat(first.key(), is("a"));
            assertThat(first.value(), is("{\"value\" : \"Lovely day for a Guinness\"}"));

            final ConsumerRecord<String, String> second = getLast(records.records(KAFKA_TOPIC), null);
            assertThat(second.key(), is("b"));
            assertThat(second.value(), is("{\"value\" : \"Lovely day for a Guinness\"}"));
        }
    }

    @Test
    public void shouldSendDeleteEventWithNullPayload() {
        // given
        final Message<ExampleJsonObject> message = message("someKey", null);

        try (final Consumer<String, String> consumer = getKafkaConsumer("someTestGroup")) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, KAFKA_TOPIC);

            // when
            messageSender.send(message).join();

            // then

            final ConsumerRecord<String, String> record = getSingleRecord(consumer, KAFKA_TOPIC, 250L);
            assertThat(record.key(), is("someKey"));
            assertThat(record.value(), is(nullValue()));
        }
    }

    @Test
    public void shouldSendCompoundKeysWithDefaultMessageHeaders() {
        // given
        final Message<ExampleJsonObject> message = message(
                Key.of("somePartitionKey", "someCompactionKey"),
                new ExampleJsonObject("banana"));
        // given

        try (final Consumer<String, String> consumer = getKafkaConsumer("someTestGroup")) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, KAFKA_TOPIC);

            // when
            messageSender.send(message).join();

            // then
            final ConsumerRecord<String, String> record = getSingleRecord(consumer, KAFKA_TOPIC, 250L);
            assertThat(record.key(), is("someCompactionKey"));
            assertThat(record.headers().lastHeader("_synapse_msg_partitionKey").value(), is("somePartitionKey".getBytes()));
            assertThat(record.headers().lastHeader("_synapse_msg_compactionKey").value(), is("someCompactionKey".getBytes()));
        }
    }

    @Test
    public void shouldSendCustomMessageHeaders() {
        // given
        final Message<ExampleJsonObject> message = message(
                "someKey",
                Header.of(of("first", "one", "second", "two")),
                new ExampleJsonObject("banana"));
        // given

        try (final Consumer<String, String> consumer = getKafkaConsumer("someTestGroup")) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, KAFKA_TOPIC);

            // when
            messageSender.send(message).join();

            // then
            final ConsumerRecord<String, String> record = getSingleRecord(consumer, KAFKA_TOPIC, 250L);
            assertThat(record.key(), is("someKey"));
            assertThat(record.value(), is("{\"value\":\"banana\"}"));
            assertThat(record.headers().lastHeader("first").value(), is("one".getBytes()));
            assertThat(record.headers().lastHeader("second").value(), is("two".getBytes()));
        }
    }

    private Consumer<String, String> getKafkaConsumer(final String consumerGroup) {
        final Map<String, Object> consumerProps = consumerProps(consumerGroup, "true", this.embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        final ConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);

        return cf.createConsumer();
    }

    @Configuration
    @EnableKafkaStreams
    public static class TestConfiguration {

        @Value("${" + SPRING_EMBEDDED_KAFKA_BROKERS + "}")
        private String brokerAddresses;

        @Bean(name = DEFAULT_STREAMS_CONFIG_BEAN_NAME)
        public KafkaStreamsConfiguration kStreamsConfigs() {
            final Map<String, Object> props = new HashMap<>();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
            return new KafkaStreamsConfiguration(props);
        }

        @Bean
        public ProducerFactory<String, String> producerFactory() {
            return new DefaultKafkaProducerFactory<>(producerConfigs());
        }

        @Bean
        public Map<String, Object> producerConfigs() {
            final Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            // See https://kafka.apache.org/documentation/#producerconfigs for more properties
            return props;
        }

        @Bean
        public KafkaTemplate<String, String> kafkaTemplate() {
            return new KafkaTemplate<>(producerFactory());
        }
    }

    private static class ExampleJsonObject {
        @JsonProperty
        private String value;

        public ExampleJsonObject() {
        }

        ExampleJsonObject(String value) {
            this.value = value;
        }

    }

}