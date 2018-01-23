package de.otto.edison.eventsourcing;

import org.junit.Test;
import org.springframework.security.crypto.encrypt.Encryptors;
import org.springframework.security.crypto.encrypt.TextEncryptor;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class TemporaryDecryptionTest {


    private TextEncryptor encryptor = Encryptors.text("pass", "abba");

    @Test
    public void shouldNotDecryptBecauseNotEncrypted() {
        String entry = "{\"id\": \"someId\", \"otherDings\": \"bla\" }";

        String out = TemporaryDecryption.decryptIfNecessary(entry, encryptor);

        assertThat(out, is(entry));
    }

    @Test
    public void shouldDecryptBecauseEncrypted() {
        String entry = "{\"id\": \"someId\", \"otherDings\": \"bla\" }";
        String encryptedEntry = encryptor.encrypt(entry);

        String out = TemporaryDecryption.decryptIfNecessary(encryptedEntry, encryptor);

        assertThat(out, is(entry));
    }

}