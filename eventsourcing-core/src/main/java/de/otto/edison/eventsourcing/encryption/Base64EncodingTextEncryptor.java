package de.otto.edison.eventsourcing.encryption;

import com.google.common.base.Charsets;
import org.springframework.security.crypto.codec.Base64;
import org.springframework.security.crypto.codec.Utf8;
import org.springframework.security.crypto.encrypt.BytesEncryptor;
import org.springframework.security.crypto.encrypt.TextEncryptor;

public class Base64EncodingTextEncryptor implements TextEncryptor {

    private final BytesEncryptor encryptor;

    public Base64EncodingTextEncryptor(BytesEncryptor encryptor) {
        this.encryptor = encryptor;
    }

    public String encrypt(String text) {
        return new String(Base64.encode(encryptor.encrypt(Utf8.encode(text))), Charsets.UTF_8);
    }

    public String decrypt(String encryptedText) {
        return Utf8.decode(encryptor.decrypt(Base64.decode(encryptedText.getBytes(Charsets.UTF_8))));
    }

}