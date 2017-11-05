package de.otto.edison.eventsourcing.example.consumer.configuration;

import de.otto.edison.eventsourcing.annotation.EnableEventSource;
import de.otto.edison.eventsourcing.example.consumer.ExampleConsumer;
import de.otto.edison.eventsourcing.example.consumer.payload.BananaPayload;
import de.otto.edison.eventsourcing.example.consumer.payload.ProductPayload;
import de.otto.edison.eventsourcing.example.consumer.state.BananaProduct;
import de.otto.edison.eventsourcing.state.DefaultStateRepository;
import de.otto.edison.eventsourcing.state.StateRepository;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

    /*
     * TODO: 1.
     * TODO: Move KinesisConfiguration to edison-aws
     *
     * TODO: 2.
     * TODO: Wenn man den example consumer startet, sieht man jede Menge "KinesisEventSource" message="Consumed 0 records from kinesis"
     * TODO: Log messages. Warum 0 records??
     *
     * TODO: 3.
     * TODO: Shutdown funktioniert noch nicht. EventSourceConsumerProcess reagiert nicht auf Shutdown Signal.
     *
     * TODO: 4.
     * TODO: EventSourceConsumerProcess testen.
     *
     */


@Configuration
@EnableConfigurationProperties({MyServiceProperties.class})
@EnableEventSource(
        name = "productEventSource",
        streamName = "${exampleservice.product-stream-name}",
        payloadType = ProductPayload.class)
@EnableEventSource(
        name = "bananaEventSource",
        streamName = "${exampleservice.banana-stream-name}",
        payloadType = BananaPayload.class)
public class ExampleConfiguration {

    @Bean
    public StateRepository<BananaProduct> bananaProductStateRepository() {
        return new DefaultStateRepository<>();
    }

    @Bean
    public ExampleConsumer exampleConsumer(final StateRepository<BananaProduct> stateRepository) {
        return new ExampleConsumer(stateRepository);
    }
}
