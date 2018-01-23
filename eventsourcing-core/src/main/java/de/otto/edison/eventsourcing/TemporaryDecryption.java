package de.otto.edison.eventsourcing;

import org.springframework.security.crypto.encrypt.TextEncryptor;

@Deprecated
public class TemporaryDecryption {

    public static String decryptIfNecessary(String entry, TextEncryptor textEncryptor) {
        if (entry.startsWith("{")) {
            return entry;
        } else {
            return textEncryptor.decrypt(entry);
        }
    }

}
