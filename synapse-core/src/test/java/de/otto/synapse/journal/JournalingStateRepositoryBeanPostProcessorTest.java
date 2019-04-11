package de.otto.synapse.journal;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import de.otto.synapse.annotation.EnableEventSource;
import de.otto.synapse.annotation.EventSourceConsumer;
import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.configuration.InMemoryMessageLogTestConfiguration;
import de.otto.synapse.configuration.SynapseAutoConfiguration;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.MessageInterceptorRegistration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.messagestore.InMemoryMessageStore;
import de.otto.synapse.messagestore.Index;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.state.ConcurrentMapStateRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static de.otto.synapse.messagestore.Indexers.journalKeyIndexer;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.core.Ordered.LOWEST_PRECEDENCE;

public class JournalingStateRepositoryBeanPostProcessorTest {

    private static final Logger LOG = getLogger(JournalingStateRepositoryBeanPostProcessorTest.class);
    private AnnotationConfigApplicationContext context;

    @Before
    public void init() {
        context = new AnnotationConfigApplicationContext();
        TestPropertyValues.of(
                "synapse.sender.default-headers.enabled=false",
                "synapse.receiver.default-headers.enabled=false")
                .applyTo(context);
    }

    @After
    public void close() {
        if (this.context != null) {
            this.context.close();
        }
    }

    @Test
    public void shouldRegisterBeanPostProcessorWithLowestPrecedence() {
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.register(SynapseAutoConfiguration.class);
        context.refresh();

        final JournalingStateRepositoryBeanPostProcessor postProcessor = context.getBean(JournalingStateRepositoryBeanPostProcessor.class);

        assertThat(postProcessor.getOrder(), is(LOWEST_PRECEDENCE));
    }

    @Test
    public void shouldRegisterInterceptorForSingleConsumer() {
        context.register(SingleConsumerJournaledStateRepository.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.register(SynapseAutoConfiguration.class);
        context.refresh();

        final MessageInterceptorRegistry registry = context.getBean(MessageInterceptorRegistry.class);

        final ImmutableList<MessageInterceptorRegistration> registrations = registry.getRegistrations("test-channel", EndpointType.RECEIVER);
        assertThat(registrations.size(), is(1));
        assertThat(registrations.get(0).getInterceptor(), is(instanceOf(JournalingInterceptor.class)));
    }

    @Test
    public void shouldProcessLotsOfMessages() throws InterruptedException {
        final long updatesPerEntity = 100;
        final long numEntities = 100;

        context.register(SingleConsumerJournaledStateRepository.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.register(SynapseAutoConfiguration.class);
        context.refresh();

        final InMemoryChannels channels = context.getBean(InMemoryChannels.class);
        InMemoryChannel channel = channels.getChannel("test-channel");
        final Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < updatesPerEntity; i++) {
            for (int j = 0; j < numEntities; j++) {
                channel.send(someTextMessage(j));
            }
            LOG.info("{} message sent within {}ms", (i+1) * numEntities, stopwatch.elapsed(MILLISECONDS));
        }

        final Journal journal = context.getBean(JournalingStateRepository.class);
        channel.send(someTextMessage(-1));
        await()
                .atMost(5, MINUTES)
                .until(()->journal.getJournalFor("key:-1").findAny().isPresent());

        assertThat(journal.getJournalFor("key:4").count(), is(updatesPerEntity));

        final MessageStore messageStore = journal.getMessageStore();
        final int expectedSize = toIntExact(1 + numEntities * updatesPerEntity);
        assertThat(messageStore.size(), is(expectedSize));
        assertThat(messageStore.stream(Index.JOURNAL_KEY, "key:4").count(), is(updatesPerEntity));
    }

    private TextMessage someTextMessage(final int i) {
        return TextMessage.of("key:" + i, "some payload " + i);
    }

    @EnableEventSource(name = "test", channelName = "test-channel")
    public static class SingleConsumerJournaledStateRepository extends JournalingStateRepository {
        public SingleConsumerJournaledStateRepository() {
            super(
                    new ConcurrentMapStateRepository("test"),
                    new InMemoryMessageStore(journalKeyIndexer())
            );
        }

        @EventSourceConsumer(eventSource = "test", payloadType = String.class)
        public void test(final Message<String> message) {
        }

        @Override
        public String journalKeyOf(String entityId) {
            return entityId;
        }
    }

}

