package de.otto.edison.eventsourcing.example.consumer.configuration;

import de.otto.edison.eventsourcing.annotation.EnableEventSource;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.EventSourceConsumerProcess;
import de.otto.edison.eventsourcing.example.consumer.payload.BananaPayload;
import de.otto.edison.eventsourcing.example.consumer.payload.ProductPayload;
import de.otto.edison.eventsourcing.example.consumer.state.BananaProduct;
import de.otto.edison.eventsourcing.state.DefaultStateRepository;
import de.otto.edison.eventsourcing.state.StateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


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
     * TODO: 5.
     * TODO: Eine Annotation @EventConsumer könnte eine x-beliebige Funktion zum EventConsumer machen. Über
     * TODO: ein property der Annotation könnte man diesen auch direkt an eine EventSource hängen.
     *
     */

    private static Logger LOG = LoggerFactory.getLogger(ExampleConfiguration.class);

    @Autowired
    private MyServiceProperties properties;

    /***************************
     * The State Repository:   *
     ***************************/

    @Bean
    public StateRepository<BananaProduct> bananaProductStateRepository() {
        return new DefaultStateRepository<>();
    }

    /***************************
     * Consume Product Events: *
     ***************************/

    @Bean
    public EventSourceConsumerProcess productConsumerProcess(final EventSource<ProductPayload> productEventSource,
                                                             final EventConsumer<ProductPayload> productEventConsumer) {
        return new EventSourceConsumerProcess(productEventSource, productEventConsumer);
    }

    /***************************
     * Consume Banana Events: *
     ***************************/

    @Bean
    public EventSourceConsumerProcess bananaConsumerProcess(final EventSource<BananaPayload> bananaEventSource,
                                                            final EventConsumer<BananaPayload> bananaEventConsumer) {
        return new EventSourceConsumerProcess(bananaEventSource, bananaEventConsumer);
    }

}
