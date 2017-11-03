package de.otto.edison.eventsourcing.example.consumer.configuration;

import de.otto.edison.eventsourcing.EventSourceFactory;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.EventSourceConsumerProcess;
import de.otto.edison.eventsourcing.example.consumer.payload.BananaPayload;
import de.otto.edison.eventsourcing.example.consumer.payload.ProductPayload;
import de.otto.edison.eventsourcing.example.consumer.state.BananaProduct;
import de.otto.edison.eventsourcing.state.DefaultStateRepository;
import de.otto.edison.eventsourcing.state.StateRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static de.otto.edison.eventsourcing.example.consumer.state.BananaProduct.bananaProductBuilder;


@Configuration
@EnableConfigurationProperties({MyServiceProperties.class})
public class ExampleConfiguration {

    /*
     * TODO: 0.
     * TODO: Move KinesisConfiguration to edison-aws
     * TODO: Extract edison-eventsourcing library
     * TODO: Extract example into sub-project / separate module
     *
     * TODO: 1.
     * TODO: Server startet noch nicht, da die CompactionProperties nicht gefunden werden.
     *
     * TODO: 2.
     * TODO: EventSourceConsumerProcess testen.
     *
     * TODO: 3.
     * TODO: Eine Annotation @EventConsumer könnte eine x-beliebige Funktion zum EventConsumer machen. Über
     * TODO: ein property der Annotation könnte man diesen auch direkt an eine EventSource hängen.
     *
     * TODO: 4.
     * TODO: Properties, die pro event stream die Konfiguration enthalten (s3 bucket, etc) + eine Factory, die
     * TODO: auf Basis der properties das ganze Geraffel oben übernimmt. Am Ende sollte es reichen, die properties
     * TODO: zu setzen und eine Methode mit @EventConsumer zu annotieren.
     */


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
    public EventSource<ProductPayload> productEventSource(final EventSourceFactory factory) {
        return factory.compactedEventSource(properties.getProductStreamName(), ProductPayload.class);
    }

    @Bean
    public EventConsumer<ProductPayload> productEventConsumer(final StateRepository<BananaProduct> bananaProductStateRepository) {
        return event -> bananaProductStateRepository.compute(event.key(), (s, bananaProduct) -> {
            final BananaProduct.Builder builder = bananaProduct != null
                    ? bananaProductBuilder(bananaProduct)
                    : bananaProductBuilder();
            return builder
                    .withId(event.key())
                    .withPrice(event.payload().getPrice())
                    .build();
        });
    }

    @Bean
    public EventSourceConsumerProcess productConsumerProcess(final EventSource<ProductPayload> productEventSource,
                                                             final EventConsumer<ProductPayload> productEventConsumer) {
        return new EventSourceConsumerProcess(productEventSource, productEventConsumer);
    }

    /***************************
     * Consume Banana Events: *
     ***************************/


    @Bean
    public EventSource<BananaPayload> bananaEventSource(final EventSourceFactory factory) {
        return factory.compactedEventSource(properties.getBananaStreamName(), BananaPayload.class);
    }

    @Bean
    public EventConsumer<BananaPayload> bananaEventConsumer(final StateRepository<BananaProduct> bananaProductStateRepository) {
        return event -> bananaProductStateRepository.compute(event.key(), (s, bananaProduct) -> {
            final BananaProduct.Builder builder = bananaProduct != null
                    ? bananaProductBuilder(bananaProduct)
                    : bananaProductBuilder();
            return builder
                        .withId(event.key())
                        .withColor(event.payload().getColor())
                        .build();
        });
    }

    @Bean
    public EventSourceConsumerProcess bananaConsumerProcess(final EventSource<BananaPayload> bananaEventSource,
                                                            final EventConsumer<BananaPayload> bananaEventConsumer) {
        return new EventSourceConsumerProcess(bananaEventSource, bananaEventConsumer);
    }

}
