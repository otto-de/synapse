package de.otto.edison.eventsourcing.inmemory;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.encrypt.TextEncryptor;

@Configuration
public class InMemoryConfiguration {

    @Bean
    public InMemoryStream testStream() {
        return new InMemoryStream();
    }

    @Bean
    public InMemoryEventSender testStreamSender(ObjectMapper objectMapper, TextEncryptor textEncryptor) {
        return new InMemoryEventSender("test", objectMapper, textEncryptor, testStream());
    }

    @Bean InMemoryEventSource testEventSource() {

    }

}
