package de.otto.synapse.example.edison.configuration;

import de.otto.edison.status.indicator.StatusDetailIndicator;
import de.otto.synapse.annotation.EnableEventSource;
import de.otto.synapse.annotation.EnableMessageQueueReceiverEndpoint;
import de.otto.synapse.annotation.EnableMessageSenderEndpoint;
import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.configuration.InMemoryMessageLogTestConfiguration;
import de.otto.synapse.configuration.InMemoryMessageQueueTestConfiguration;
import de.otto.synapse.edison.statusdetail.StateRepositoryStatusDetailIndicator;
import de.otto.synapse.example.edison.state.BananaProduct;
import de.otto.synapse.example.edison.state.EventConsumers;
import de.otto.synapse.journal.Journal;
import de.otto.synapse.state.ConcurrentMapStateRepository;
import de.otto.synapse.state.StateRepository;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static de.otto.synapse.journal.Journals.multiChannelJournal;
import static org.slf4j.LoggerFactory.getLogger;


@Configuration
@EnableConfigurationProperties(MyServiceProperties.class)
@Import({
        InMemoryMessageLogTestConfiguration.class,
        InMemoryMessageQueueTestConfiguration.class})
@EnableEventSource(
        name = "bananaSource",
        channelName = "${exampleservice.banana-channel}")
@EnableEventSource(
        name = "productSource",
        channelName = "${exampleservice.product-channel}")
@EnableMessageQueueReceiverEndpoint(
        name = "bananaQueue",
        channelName = "banana-queue")
@EnableMessageSenderEndpoint(
        name = "bananaMessageSender",
        channelName = "${exampleservice.banana-channel}",
        selector = MessageLog.class)
@EnableMessageSenderEndpoint(
        name = "productMessageSender",
        channelName = "${exampleservice.product-channel}",
        selector = MessageLog.class)
public class ExampleConfiguration {

    private static final Logger LOG = getLogger(ExampleConfiguration.class);

    @Bean
    public StateRepository<BananaProduct> bananaProductStateRepository() {
        return new ConcurrentMapStateRepository<>("BananaProducts");
    }

    @Bean
    public EventConsumers eventConsumers(final StateRepository<BananaProduct> bananaProductStateRepository) {
        return new EventConsumers(bananaProductStateRepository);
    }

    @Bean
    public Journal bananaProductJournal(final StateRepository<BananaProduct> bananaProductStateRepository,
                                        @Value("${exampleservice.banana-channel}") String bananaChannel,
                                        @Value("${exampleservice.product-channel}") String productChannel) {
        return multiChannelJournal(bananaProductStateRepository, bananaChannel, productChannel);
    }

    @Bean
    public StatusDetailIndicator bananaProductStatusDetailIndicator(StateRepository<BananaProduct> bananaProductStateRepository) {
        return new StateRepositoryStatusDetailIndicator(bananaProductStateRepository, "bananaProducts");
    }

}
