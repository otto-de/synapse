package de.otto.edison.eventsourcing.encryption;

import org.junit.Test;
import org.springframework.security.crypto.codec.Hex;
import org.springframework.security.crypto.encrypt.Encryptors;

import java.io.UnsupportedEncodingException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class EncryptedOrPlainJsonTextEncryptorTest {

    @Test
    public void shouldNotDecryptPlainJson() throws UnsupportedEncodingException {
        EncryptedOrPlainJsonTextEncryptor encryptor = createEncryptor();

        String decryptedText = encryptor.decrypt("{value: \"test\"}");

        assertThat(decryptedText, is("{value: \"test\"}"));
    }

    @Test
    public void shouldDecryptEncryptedJson() throws UnsupportedEncodingException {
        EncryptedOrPlainJsonTextEncryptor encryptor = createEncryptor();

        String encryptedText = encryptor.encrypt("{value: \"test\"}");
        String decryptedText = encryptor.decrypt(encryptedText);

        assertThat(decryptedText, is("{value: \"test\"}"));
    }

    private EncryptedOrPlainJsonTextEncryptor createEncryptor() throws UnsupportedEncodingException {
        String hexEncodedSalt = String.valueOf(Hex.encode("mySalt".getBytes("UTF-8")));
        Base64EncodingTextEncryptor delegateEncryptor = new Base64EncodingTextEncryptor(Encryptors.standard("myPassword", hexEncodedSalt));
        return new EncryptedOrPlainJsonTextEncryptor(delegateEncryptor);
    }
}