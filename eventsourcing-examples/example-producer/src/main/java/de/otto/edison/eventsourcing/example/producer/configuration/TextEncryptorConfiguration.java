package de.otto.edison.eventsourcing.example.producer.configuration;

import de.otto.edison.eventsourcing.encryption.Base64EncodingTextEncryptor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.codec.Hex;
import org.springframework.security.crypto.encrypt.Encryptors;
import org.springframework.security.crypto.encrypt.TextEncryptor;

import java.io.UnsupportedEncodingException;

@Configuration
public class TextEncryptorConfiguration {

    @Bean
    public TextEncryptor textEncryptor(
            @Value("${service.encryption.password}") String password,
            @Value("${service.encryption.salt}") String salt
    ) throws UnsupportedEncodingException {
        String hexEncodedSalt = new String(Hex.encode(salt.getBytes("UTF-8")));
        return new Base64EncodingTextEncryptor(Encryptors.standard(password, hexEncodedSalt));
    }
}
