package de.otto.edison.eventsourcing.encryption;

import org.springframework.security.crypto.encrypt.TextEncryptor;

public class EncryptedOrPlainJsonTextEncryptor implements TextEncryptor {

    private final Base64EncodingTextEncryptor delegate;

    public EncryptedOrPlainJsonTextEncryptor(Base64EncodingTextEncryptor delegate) {
        this.delegate = delegate;
    }

    public String encrypt(String text) {
        return delegate.encrypt(text);
    }

    public String decrypt(String encryptedText) {
        if (encryptedText.startsWith("{")) {
            return encryptedText;
        } else {
            return delegate.decrypt(encryptedText);
        }

    }

}