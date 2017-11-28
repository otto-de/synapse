package de.otto.edison.eventsourcing.example.producer.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.kinesis.KinesisStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import software.amazon.awssdk.services.kinesis.KinesisClient;

@Configuration
public class ExampleConfiguration {

    @Bean
    public KinesisStream kinesisStream(KinesisClient kinesisClient,
                                       MyServiceProperties serviceProperties,
                                       ObjectMapper objectMapper,
                                       TextEncryptor textEncryptor) {
        return new KinesisStream(kinesisClient, serviceProperties.getProductStreamName(), objectMapper, textEncryptor);
    }

}
