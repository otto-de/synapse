package de.otto.synapse.testsupport;

import de.otto.synapse.endpoint.SqsClientHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutionException;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;

/**
 * This class is a data source for supplying input to the Amazon Kinesis stream. It reads lines from the
 * input file specified in the constructor and emits them by calling String.getBytes() into the
 * stream defined in the KinesisConnectorConfiguration.
 */
public class SqsTestStreamSource {

    private static final Logger LOG = LoggerFactory.getLogger(SqsTestStreamSource.class);

    private final String channelName;
    private final SqsClientHelper sqsClient;
    private final String inputFile;

    public SqsTestStreamSource(SqsClientHelper sqsClient, String channelName, String inputFile) {
        this.sqsClient = sqsClient;
        this.inputFile = inputFile;
        this.channelName = channelName;
    }

    public void writeToStream() {

        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(inputFile);
        if (inputStream == null) {
            throw new IllegalStateException("Could not find input file: " + inputFile);
        }
        try {
            processInputStream(channelName, inputStream);
        } catch (Exception e) {
            LOG.error("Encountered exception while putting data in source stream.", e);
        }
    }

    protected void processInputStream(final String channelName, final InputStream inputStream) throws IOException, ExecutionException, InterruptedException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                sqsClient.sendMessage(channelName, "some-message", line);
            }
        }
    }

}
