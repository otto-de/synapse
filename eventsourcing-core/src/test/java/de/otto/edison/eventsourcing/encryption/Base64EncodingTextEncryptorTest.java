package de.otto.edison.eventsourcing.encryption;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.security.crypto.codec.Hex;
import org.springframework.security.crypto.encrypt.Encryptors;


@RunWith(DataProviderRunner.class)
public class Base64EncodingTextEncryptorTest {

    @Test
    @UseDataProvider("provideTextToBeEncoded")
    public void shouldEncodeAndDecode(String givenText) throws Exception {
        String password = "test";
        String salt = "test";
        String hexEncodedSalt = new String(Hex.encode(salt.getBytes("UTF-8")));
        Base64EncodingTextEncryptor encryptor = new Base64EncodingTextEncryptor(Encryptors.standard(password, hexEncodedSalt));

        String encryptedText = encryptor.encrypt(givenText);
        String decryptedText = encryptor.decrypt(encryptedText);

        Assert.assertEquals(givenText, decryptedText);
    }

    @DataProvider
    public static Object[][] provideTextToBeEncoded() {
        return new Object[][]{
                {"Hello World"},
                {"Hällo Wörld"},
                {"1234567890abcdefghijklmnopqrstuvwxyz"},
                {"!@#$%^&*()_+`~{}|[]<>:\",./\\"},
                {""}
        };
    }
}