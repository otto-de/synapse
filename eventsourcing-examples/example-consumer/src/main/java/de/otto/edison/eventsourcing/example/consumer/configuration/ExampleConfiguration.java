package de.otto.edison.eventsourcing.example.consumer.configuration;

import de.otto.edison.eventsourcing.annotation.EnableEventSource;
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
     * TODO: KinesisUtils + KinesisConfiguration gehören eher nach edison-aws. Statt sehr spezieller
     * TODO: Methoden, würde man sich vermutlich eher eine high-level Client-Lib wünschen, mit der man
     * TODO: (ähnlich einer EventSource) events aus Kinesis konsumieren kann, ohne sich um shards etc.
     * TODO: kümmern zu müssen.
     *
     * TODO: 2.
     * TODO: Die KinesisEventSource / das Example braucht eine Ewigkeit, um alle Events aus Kinesis zu
     * TODO: konsumieren. Zwischendurch wird vielfach gewartet, während nichts kommt, obwohl das Lag
     * TODO: noch mehrere Stunden beträgt.
     *
     * TODO: 4.
     * TODO: Testabdeckung erhöhen + JavaDocs + Examples
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

}
