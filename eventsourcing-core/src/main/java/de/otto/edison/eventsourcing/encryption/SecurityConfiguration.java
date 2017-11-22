package de.otto.edison.eventsourcing.encryption;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.encrypt.Encryptors;
import org.springframework.security.crypto.encrypt.TextEncryptor;

@Configuration
public class SecurityConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public TextEncryptor textEncryptor() {
        return  Encryptors.noOpText();
    }
}
